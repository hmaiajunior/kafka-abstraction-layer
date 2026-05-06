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
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.util.List;
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
 * Integration test: produce one message and assert that the same correlationId
 * appears in the DeliveryResult, the Micrometer counter, and the OTel span attribute.
 */
@Testcontainers
class ObservabilityCorrelationTest {

    @Container
    static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    private static final String TOPIC = "observability-correlation-it";
    private static final String CLUSTER_NAME = "obs-test-cluster";

    @BeforeAll
    static void createTopic() throws Exception {
        try (AdminClient admin = AdminClient.create(
                Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            admin.createTopics(java.util.List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get();
        }
    }

    @Test
    void produce_correlationIdAppearsInMetricsAndTraces() throws Exception {
        // === Setup OTel with in-memory exporter ===
        InMemorySpanExporter spanExporter = InMemorySpanExporter.create();
        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                .build();
        OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .build();

        // === Setup Micrometer ===
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

        // === Wire SDK ===
        TraceEmitter traceEmitter = new TraceEmitter(openTelemetry);
        MetricsEmitter metricsEmitter = new MetricsEmitter(meterRegistry, CLUSTER_NAME);
        ObservabilityContext obs = new ObservabilityContext(metricsEmitter, traceEmitter);

        SchemaValidator mockValidator = mock(SchemaValidator.class);

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.ByteArraySerializer.class.getName());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        KafkaProducer<String, byte[]> rawProducer = new KafkaProducer<>(props);
        KafkaProducerImpl producer = new KafkaProducerImpl(rawProducer, mockValidator, obs);

        // === Produce ===
        Message msg = Message.forTopic(TOPIC).payload("correlation-test-event").build();
        DeliveryResult result = producer.produce(msg).get();

        // === Assert DeliveryResult ===
        assertThat(result.isSuccess()).isTrue();
        String correlationId = result.getCorrelationId();
        assertThat(correlationId).isNotBlank();

        // === Assert OTel span carries the correlationId ===
        tracerProvider.forceFlush();
        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertThat(spans).isNotEmpty();

        SpanData produceSpan = spans.stream()
                .filter(s -> "kafka.produce".equals(s.getName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No kafka.produce span found"));

        String spanCorrelationId = produceSpan.getAttributes()
                .get(io.opentelemetry.api.common.AttributeKey.stringKey("kafka.sdk.correlation_id"));
        assertThat(spanCorrelationId).isEqualTo(correlationId);

        // === Assert Micrometer recorded a success counter for this topic ===
        List<Meter> meters = meterRegistry.getMeters();
        boolean successCounterFound = meters.stream()
                .anyMatch(m -> m.getId().getName().equals("kafka_sdk_messages_produced_total")
                        && "success".equals(m.getId().getTag("outcome"))
                        && TOPIC.equals(m.getId().getTag("topic"))
                        && CLUSTER_NAME.equals(m.getId().getTag("cluster")));
        assertThat(successCounterFound)
                .as("Expected kafka_sdk_messages_produced_total{outcome=success,topic=%s,cluster=%s}",
                        TOPIC, CLUSTER_NAME)
                .isTrue();

        producer.close();
        tracerProvider.close();
    }
}
