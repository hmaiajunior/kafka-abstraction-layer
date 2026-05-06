package com.kafka.sdk.integration;

import static org.assertj.core.api.Assertions.assertThat;

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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
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
 * Integration test: produce 500 messages to the same topic and verify that
 * the schema cache serves all but the first validation from cache (exactly 1 registry call).
 */
@Testcontainers
class SchemaCacheHitIntegrationTest {

    @Container
    static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    private static final String TOPIC = "events-cache-hit-it";
    private static final int MESSAGE_COUNT = 500;

    private static final String SCHEMA_JSON = """
            {
              "type": "record",
              "name": "Event",
              "fields": [
                {"name": "eventId", "type": "string"},
                {"name": "eventType", "type": "string"}
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
    void produce_500Messages_schemaCacheServes499OfThem() throws Exception {
        Schema schema = new Schema.Parser().parse(SCHEMA_JSON);

        MockSchemaRegistryClient srClient = new MockSchemaRegistryClient();
        srClient.register(TOPIC + "-value", new AvroSchema(schema));

        SchemaRegistryConfig srConfig = SchemaRegistryConfig.builder()
                .url("http://mock-registry")
                .cacheMaxSize(200)
                .cacheTtlSeconds(600)
                .build();
        ConfluentSchemaValidator validator = new ConfluentSchemaValidator(srClient, srConfig);

        TraceEmitter traceEmitter = new TraceEmitter(OpenTelemetry.noop());
        MetricsEmitter metricsEmitter = new MetricsEmitter(new SimpleMeterRegistry(), "cache-hit-it");
        ObservabilityContext obs = new ObservabilityContext(metricsEmitter, traceEmitter);

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.ByteArraySerializer.class.getName());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "1");

        KafkaProducer<String, byte[]> rawProducer = new KafkaProducer<>(props);
        KafkaProducerImpl producer = new KafkaProducerImpl(rawProducer, validator, obs);

        List<CompletableFuture<DeliveryResult>> futures = new ArrayList<>();
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            GenericRecord event = new GenericData.Record(schema);
            event.put("eventId", "evt-" + i);
            event.put("eventType", "click");
            Message msg = Message.forTopic(TOPIC).payload(event).build();
            futures.add(producer.produce(msg));
        }

        for (CompletableFuture<DeliveryResult> f : futures) {
            assertThat(f.get().isSuccess()).isTrue();
        }

        // First call caused a cache miss (registry lookup); all subsequent calls hit the cache
        assertThat(validator.getCache().getMissCount()).isEqualTo(1);
        assertThat(validator.getCache().getHitCount()).isEqualTo(MESSAGE_COUNT - 1);

        producer.close();
    }
}
