package com.kafka.sdk.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.kafka.sdk.model.DeliveryResult;
import com.kafka.sdk.model.Message;
import com.kafka.sdk.observability.MetricsEmitter;
import com.kafka.sdk.observability.ObservabilityContext;
import com.kafka.sdk.observability.TraceEmitter;
import com.kafka.sdk.producer.KafkaProducerImpl;
import com.kafka.sdk.schema.ConfluentSchemaValidator;
import com.kafka.sdk.config.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Integration test: produce a valid Avro GenericRecord with real Kafka (TestContainers)
 * and an in-process MockSchemaRegistryClient. Verifies end-to-end schema validation path.
 */
@Testcontainers
class SchemaValidIntegrationTest {

    @Container
    static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    private static final String TOPIC = "orders-schema-valid-it";

    private static final String SCHEMA_JSON = """
            {
              "type": "record",
              "name": "Order",
              "fields": [
                {"name": "id", "type": "string"},
                {"name": "amount", "type": "double"}
              ]
            }
            """;

    @BeforeAll
    static void createTopic() throws Exception {
        try (AdminClient admin = AdminClient.create(
                Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            admin.createTopics(java.util.List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get();
        }
    }

    @Test
    void produce_validAvroRecord_deliveredSuccessfully() throws Exception {
        Schema schema = new Schema.Parser().parse(SCHEMA_JSON);

        MockSchemaRegistryClient srClient = new MockSchemaRegistryClient();
        srClient.register(TOPIC + "-value", new AvroSchema(schema));

        SchemaRegistryConfig srConfig = SchemaRegistryConfig.builder()
                .url("http://mock-registry")
                .build();
        ConfluentSchemaValidator validator = new ConfluentSchemaValidator(srClient, srConfig);

        TraceEmitter traceEmitter = new TraceEmitter(OpenTelemetry.noop());
        MetricsEmitter metricsEmitter = new MetricsEmitter(new SimpleMeterRegistry(), "schema-valid-it");
        ObservabilityContext obs = new ObservabilityContext(metricsEmitter, traceEmitter);

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.ByteArraySerializer.class.getName());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        KafkaProducer<String, byte[]> rawProducer = new KafkaProducer<>(props);
        KafkaProducerImpl producer = new KafkaProducerImpl(rawProducer, validator, obs);

        GenericRecord order = new GenericData.Record(schema);
        order.put("id", "order-001");
        order.put("amount", 99.95);

        Message msg = Message.forTopic(TOPIC).payload(order).build();
        DeliveryResult result = producer.produce(msg).get();

        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getOffset()).isGreaterThanOrEqualTo(0);
        assertThat(result.getCorrelationId()).isNotBlank();

        producer.close();
    }
}
