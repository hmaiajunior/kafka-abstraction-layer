package com.kafka.sdk.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.kafka.sdk.KafkaProducer;
import com.kafka.sdk.config.AuthConfig;
import com.kafka.sdk.config.AuthMechanism;
import com.kafka.sdk.config.ClusterConfig;
import com.kafka.sdk.config.ClusterType;
import com.kafka.sdk.config.SchemaRegistryConfig;
import com.kafka.sdk.model.DeliveryResult;
import com.kafka.sdk.model.Message;
import com.kafka.sdk.observability.MetricsEmitter;
import com.kafka.sdk.observability.ObservabilityContext;
import com.kafka.sdk.observability.TraceEmitter;
import com.kafka.sdk.producer.KafkaClientFactory;
import com.kafka.sdk.producer.KafkaProducerImpl;
import com.kafka.sdk.schema.ConfluentSchemaValidator;
import com.kafka.sdk.schema.SchemaValidator;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class CoreProduceIntegrationTest {

    @Container
    static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    private static final String TOPIC = "orders-it";

    @BeforeAll
    static void createTopic() throws Exception {
        try (AdminClient admin = AdminClient.create(
                Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            admin.createTopics(java.util.List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get();
        }
    }

    @Test
    void produce_deliversMessageToKafkaSuccessfully() throws Exception {
        // Mock schema validator to bypass SR in this test
        SchemaValidator schemaValidator = mock(SchemaValidator.class);

        TraceEmitter traceEmitter = new TraceEmitter(OpenTelemetry.noop());
        MetricsEmitter metricsEmitter = new MetricsEmitter(new SimpleMeterRegistry(), "integration-test");
        ObservabilityContext obs = new ObservabilityContext(metricsEmitter, traceEmitter);

        // Build a ClusterConfig pointing to the TestContainers Kafka (PLAINTEXT)
        ClusterConfig config = buildPlaintextConfig();

        org.apache.kafka.clients.producer.KafkaProducer<String, byte[]> rawProducer =
                buildPlaintextKafkaProducer(config);

        KafkaProducer producer = new KafkaProducerImpl(rawProducer, schemaValidator, obs);

        Message msg = Message.forTopic(TOPIC)
                .payload("integration-test-event")
                .header("source", "test")
                .build();

        DeliveryResult result = producer.produce(msg).get();

        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getTopic()).isEqualTo(TOPIC);
        assertThat(result.getOffset()).isGreaterThanOrEqualTo(0);
        assertThat(result.getCorrelationId()).isNotBlank();

        producer.close();
    }

    private ClusterConfig buildPlaintextConfig() {
        return ClusterConfig.builder()
                .name("integration-test")
                .type(ClusterType.MSK)
                .bootstrapServers(kafka.getBootstrapServers())
                .authConfig(AuthConfig.builder().mechanism(AuthMechanism.IAM).build())
                .schemaRegistryConfig(SchemaRegistryConfig.builder().url("http://localhost:8081").build())
                .build();
    }

    private org.apache.kafka.clients.producer.KafkaProducer<String, byte[]> buildPlaintextKafkaProducer(
            ClusterConfig config) {
        java.util.Properties props = new java.util.Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.ByteArraySerializer.class.getName());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        return new org.apache.kafka.clients.producer.KafkaProducer<>(props);
    }
}
