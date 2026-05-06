package com.kafka.sdk.unit;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafka.sdk.model.DeliveryResult;
import com.kafka.sdk.observability.TraceEmitter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TraceEmitterTest {

    private InMemorySpanExporter spanExporter;
    private TraceEmitter traceEmitter;

    @BeforeEach
    void setUp() {
        spanExporter = InMemorySpanExporter.create();
        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                .build();
        OpenTelemetrySdk otel = OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .build();
        traceEmitter = new TraceEmitter(otel);
    }

    @Test
    void startProduceSpan_createsSpanNamedKafkaProduce() {
        Span span = traceEmitter.startProduceSpan("corr-123", "orders");
        traceEmitter.endSpanOk(span);

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertThat(spans).hasSize(1);
        assertThat(spans.get(0).getName()).isEqualTo("kafka.produce");
    }

    @Test
    void startSchemaValidateSpan_createsChildSpan() {
        Span root = traceEmitter.startProduceSpan("corr-456", "orders");
        Span child = traceEmitter.startSchemaValidateSpan(root, "orders");
        traceEmitter.endSpanOk(child);
        traceEmitter.endSpanOk(root);

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertThat(spans).hasSize(2);
        assertThat(spans).extracting(SpanData::getName)
                .containsExactlyInAnyOrder("kafka.produce", "kafka.schema.validate");
    }

    @Test
    void endSpan_setsOutcomeAttributeOnSuccessResult() {
        Span span = traceEmitter.startProduceSpan("corr-789", "orders");
        DeliveryResult result = DeliveryResult.success("orders", "corr-789", 0, 100L);
        traceEmitter.endSpan(span, result);

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertThat(spans.get(0).getAttributes()
                .get(io.opentelemetry.api.common.AttributeKey.stringKey("kafka.sdk.outcome")))
                .isEqualTo("success");
    }

    @Test
    void startClientSendSpan_createsSpanNamedKafkaClientSend() {
        Span root = traceEmitter.startProduceSpan("corr-abc", "orders");
        Span sendSpan = traceEmitter.startClientSendSpan(root, "orders");
        traceEmitter.endSpanOk(sendSpan);
        traceEmitter.endSpanOk(root);

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertThat(spans).extracting(SpanData::getName)
                .contains("kafka.client.send");
    }
}
