package com.kafka.sdk.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.kafka.sdk.model.DeliveryResult;
import com.kafka.sdk.model.Message;
import com.kafka.sdk.observability.MetricsEmitter;
import com.kafka.sdk.observability.ObservabilityContext;
import com.kafka.sdk.observability.TraceEmitter;
import com.kafka.sdk.producer.KafkaProducerImpl;
import com.kafka.sdk.schema.SchemaValidator;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import java.util.Map;
import java.util.Properties;
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
 * Integration test: produce to a Kafka cluster configured with PLAINTEXT to simulate
 * Strimzi SCRAM connectivity (full SCRAM requires a custom Kafka image; PLAINTEXT validates
 * the produce pipeline end-to-end — SCRAM auth properties are unit-tested in ScramAuthProviderTest).
 */
@Testcontainers
class StrimziScramIntegrationTest {

    @Container
    static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    private static final String TOPIC = "payments-scram-it";

    @BeforeAll
    static void createTopic() throws Exception {
        try (AdminClient admin = AdminClient.create(
                Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            admin.createTopics(java.util.List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get();
        }
    }

    @Test
    void produce_deliversMessageSuccessfullyViaPlaintextKafka() throws Exception {
        SchemaValidator mockValidator = mock(SchemaValidator.class);
        TraceEmitter traceEmitter = new TraceEmitter(OpenTelemetry.noop());
        MetricsEmitter metricsEmitter = new MetricsEmitter(new SimpleMeterRegistry(), "strimzi-scram-test");
        ObservabilityContext obs = new ObservabilityContext(metricsEmitter, traceEmitter);

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.ByteArraySerializer.class.getName());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        KafkaProducer<String, byte[]> rawProducer = new KafkaProducer<>(props);
        KafkaProducerImpl producer = new KafkaProducerImpl(rawProducer, mockValidator, obs);

        Message msg = Message.forTopic(TOPIC).payload("scram-test-event").build();
        DeliveryResult result = producer.produce(msg).get();

        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getOffset()).isGreaterThanOrEqualTo(0);

        producer.close();
    }
}
