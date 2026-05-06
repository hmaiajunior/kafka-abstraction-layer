package com.kafka.sdk.unit;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafka.sdk.model.DeliveryResult;
import com.kafka.sdk.model.Message;
import com.kafka.sdk.observability.MetricsEmitter;
import com.kafka.sdk.observability.ObservabilityContext;
import com.kafka.sdk.observability.ObservabilityContext.CallContext;
import com.kafka.sdk.observability.TraceEmitter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

class ObservabilityContextTest {

    private ObservabilityContext ctx;

    @BeforeEach
    void setUp() {
        TraceEmitter traceEmitter = new TraceEmitter(OpenTelemetry.noop());
        MetricsEmitter metricsEmitter = new MetricsEmitter(new SimpleMeterRegistry(), "test");
        ctx = new ObservabilityContext(metricsEmitter, traceEmitter);
    }

    @Test
    void start_generatesNonBlankCorrelationId() {
        Message msg = Message.forTopic("orders").payload("p").build();
        CallContext callCtx = ctx.start(msg);

        assertThat(callCtx.correlationId).isNotBlank();
        ctx.complete(callCtx, DeliveryResult.success("orders", callCtx.correlationId, 0, 1L));
    }

    @Test
    void start_generatesUniqueCorrelationIdPerCall() {
        Set<String> ids = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            Message msg = Message.forTopic("orders").payload("p" + i).build();
            CallContext callCtx = ctx.start(msg);
            ids.add(callCtx.correlationId);
            ctx.complete(callCtx, DeliveryResult.success("orders", callCtx.correlationId, 0, (long) i));
        }
        assertThat(ids).hasSize(100);
    }

    @Test
    void complete_clearsMdcKeysAfterCall() {
        Message msg = Message.forTopic("orders").payload("p").build();
        CallContext callCtx = ctx.start(msg);

        assertThat(MDC.get("kafka.sdk.correlationId")).isNotBlank();

        ctx.complete(callCtx, DeliveryResult.success("orders", callCtx.correlationId, 0, 1L));

        assertThat(MDC.get("kafka.sdk.correlationId")).isNull();
        assertThat(MDC.get("kafka.sdk.topic")).isNull();
    }

    @Test
    void correlationId_matchesValueInDeliveryResult() {
        Message msg = Message.forTopic("orders").payload("p").build();
        CallContext callCtx = ctx.start(msg);
        DeliveryResult result = DeliveryResult.success("orders", callCtx.correlationId, 0, 1L);
        ctx.complete(callCtx, result);

        assertThat(result.getCorrelationId()).isEqualTo(callCtx.correlationId);
    }
}
