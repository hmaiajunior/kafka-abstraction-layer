package com.kafka.sdk.observability;

import com.kafka.sdk.model.DeliveryResult;
import com.kafka.sdk.model.Message;
import io.opentelemetry.api.trace.Span;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.slf4j.MDC;

/**
 * Manages per-produce-call observability context: correlation ID, MDC keys, metrics, and traces.
 * One shared instance per SDK producer; per-call state is isolated via {@link CallContext}.
 */
public class ObservabilityContext {

    private final MetricsEmitter metricsEmitter;
    private final TraceEmitter traceEmitter;

    public ObservabilityContext(MetricsEmitter metricsEmitter, TraceEmitter traceEmitter) {
        this.metricsEmitter = metricsEmitter;
        this.traceEmitter = traceEmitter;
    }

    /**
     * Called at the start of {@code produce()}. Sets MDC keys and starts the root trace span.
     *
     * @return a {@link CallContext} that must be passed to {@link #complete(CallContext, DeliveryResult)}
     */
    public CallContext start(Message message) {
        String correlationId = UUID.randomUUID().toString();
        Instant startTime = Instant.now();

        MDC.put("kafka.sdk.correlationId", correlationId);
        MDC.put("kafka.sdk.topic", message.getTopic());

        Span span = traceEmitter.startProduceSpan(correlationId, message.getTopic());
        return new CallContext(correlationId, startTime, span);
    }

    /**
     * Called when the produce call finishes (success or failure). Records metrics and ends spans.
     */
    public void complete(CallContext ctx, DeliveryResult result) {
        try {
            Duration elapsed = Duration.between(ctx.startTime, Instant.now());
            metricsEmitter.recordResult(result, elapsed);
            traceEmitter.endSpan(ctx.rootSpan, result);
            MDC.put("kafka.sdk.outcome",
                    result.isSuccess() ? "success" : result.getErrorCode().name().toLowerCase());
            if (result.isSuccess()) {
                MDC.put("kafka.sdk.offset", String.valueOf(result.getOffset()));
            }
        } finally {
            MDC.remove("kafka.sdk.correlationId");
            MDC.remove("kafka.sdk.topic");
            MDC.remove("kafka.sdk.outcome");
            MDC.remove("kafka.sdk.offset");
        }
    }

    /** Immutable per-call context holder. */
    public static final class CallContext {
        public final String correlationId;
        final Instant startTime;
        final Span rootSpan;

        CallContext(String correlationId, Instant startTime, Span rootSpan) {
            this.correlationId = correlationId;
            this.startTime = startTime;
            this.rootSpan = rootSpan;
        }
    }
}
