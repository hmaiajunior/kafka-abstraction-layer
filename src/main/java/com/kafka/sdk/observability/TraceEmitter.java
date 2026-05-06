package com.kafka.sdk.observability;

import com.kafka.sdk.model.DeliveryResult;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;

/**
 * Emits OpenTelemetry trace spans for produce operations.
 * Span names: {@code kafka.produce} (root), {@code kafka.schema.validate}, {@code kafka.client.send}.
 */
public class TraceEmitter {

    private static final String INSTRUMENTATION_NAME = "com.kafka.sdk";
    private final Tracer tracer;

    public TraceEmitter(OpenTelemetry openTelemetry) {
        this.tracer = openTelemetry.getTracer(INSTRUMENTATION_NAME, "1.0.0");
    }

    public Span startProduceSpan(String correlationId, String topic) {
        return tracer.spanBuilder("kafka.produce")
                .setAttribute("kafka.sdk.correlation_id", correlationId)
                .setAttribute("kafka.topic", topic)
                .startSpan();
    }

    public Span startSchemaValidateSpan(Span parent, String topic) {
        return tracer.spanBuilder("kafka.schema.validate")
                .setParent(io.opentelemetry.context.Context.current().with(parent))
                .setAttribute("kafka.topic", topic)
                .startSpan();
    }

    public Span startClientSendSpan(Span parent, String topic) {
        return tracer.spanBuilder("kafka.client.send")
                .setParent(io.opentelemetry.context.Context.current().with(parent))
                .setAttribute("kafka.topic", topic)
                .startSpan();
    }

    public void endSpan(Span span, DeliveryResult result) {
        if (result.isSuccess()) {
            span.setAttribute("kafka.sdk.outcome", "success");
            span.setAttribute("kafka.partition", result.getPartition());
            span.setAttribute("kafka.offset", result.getOffset());
            span.setStatus(StatusCode.OK);
        } else {
            span.setAttribute("kafka.sdk.outcome", result.getErrorCode().name());
            span.setStatus(StatusCode.ERROR, result.getErrorMessage());
        }
        span.end();
    }

    public void endSpanOk(Span span) {
        span.setStatus(StatusCode.OK);
        span.end();
    }

    public void endSpanError(Span span, String errorMessage) {
        span.setStatus(StatusCode.ERROR, errorMessage);
        span.end();
    }
}
