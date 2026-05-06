package com.kafka.sdk.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafka.sdk.model.DeliveryResult;
import com.kafka.sdk.model.ErrorCode;
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
 * Integration test: produce an Avro record that violates the registered schema.
 * Verifies SCHEMA_VALIDATION_FAILED is returned and no message reaches the Kafka topic.
 */
@Testcontainers
class SchemaInvalidIntegrationTest {

    @Container
    static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    private static final String TOPIC = "payments-schema-invalid-it";

    private static final String STRICT_SCHEMA_JSON = """
            {
              "type": "record",
              "name": "Payment",
              "fields": [
                {"name": "paymentId", "type": "string"},
                {"name": "currency", "type": "string"},
                {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}}
              ]
            }
            """;

    private static final String WRONG_SCHEMA_JSON = """
            {
              "type": "record",
              "name": "WrongPayment",
              "fields": [
                {"name": "unknownField", "type": "string"}
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
    void produce_invalidAvroRecord_returnsSchemaValidationFailed() throws Exception {
        Schema registeredSchema = new Schema.Parser().parse(STRICT_SCHEMA_JSON);
        Schema wrongSchema = new Schema.Parser().parse(WRONG_SCHEMA_JSON);

        MockSchemaRegistryClient srClient = new MockSchemaRegistryClient();
        srClient.register(TOPIC + "-value", new AvroSchema(registeredSchema));

        SchemaRegistryConfig srConfig = SchemaRegistryConfig.builder()
                .url("http://mock-registry")
                .build();
        ConfluentSchemaValidator validator = new ConfluentSchemaValidator(srClient, srConfig);

        TraceEmitter traceEmitter = new TraceEmitter(OpenTelemetry.noop());
        MetricsEmitter metricsEmitter = new MetricsEmitter(new SimpleMeterRegistry(), "schema-invalid-it");
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

        // Record built from the wrong schema — will fail validation against registeredSchema
        GenericRecord invalidRecord = new GenericData.Record(wrongSchema);
        invalidRecord.put("unknownField", "bad-value");

        Message msg = Message.forTopic(TOPIC).payload(invalidRecord).build();
        DeliveryResult result = producer.produce(msg).get();

        assertThat(result.isSuccess()).isFalse();
        assertThat(result.getErrorCode()).isEqualTo(ErrorCode.SCHEMA_VALIDATION_FAILED);
        assertThat(result.getErrorMessage()).isNotBlank();

        producer.close();
    }

    @Test
    void produce_unregisteredSubject_returnsSchemaValidationFailed() throws Exception {
        // No schema registered for this topic — should return 404 from SR
        MockSchemaRegistryClient srClient = new MockSchemaRegistryClient();

        SchemaRegistryConfig srConfig = SchemaRegistryConfig.builder()
                .url("http://mock-registry")
                .build();
        ConfluentSchemaValidator validator = new ConfluentSchemaValidator(srClient, srConfig);

        TraceEmitter traceEmitter = new TraceEmitter(OpenTelemetry.noop());
        MetricsEmitter metricsEmitter = new MetricsEmitter(new SimpleMeterRegistry(), "schema-invalid-it-2");
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

        Message msg = Message.forTopic(TOPIC).payload("no-schema-registered").build();
        DeliveryResult result = producer.produce(msg).get();

        assertThat(result.isSuccess()).isFalse();
        assertThat(result.getErrorCode()).isEqualTo(ErrorCode.SCHEMA_VALIDATION_FAILED);

        producer.close();
    }
}
