package com.kafka.sdk.unit;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafka.sdk.model.DeliveryResult;
import com.kafka.sdk.model.ErrorCode;
import com.kafka.sdk.observability.MetricsEmitter;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MetricsEmitterTest {

    private SimpleMeterRegistry registry;
    private MetricsEmitter emitter;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        emitter = new MetricsEmitter(registry, "test-cluster");
    }

    @Test
    void recordResult_incrementsSuccessCounter() {
        DeliveryResult result = DeliveryResult.success("orders", "corr-1", 0, 42L);
        emitter.recordResult(result, Duration.ofMillis(5));

        double count = registry.counter(
                "kafka_sdk_messages_produced_total",
                "topic", "orders",
                "cluster", "test-cluster",
                "outcome", "success").count();

        assertThat(count).isEqualTo(1.0);
    }

    @Test
    void recordResult_incrementsFailureCounter() {
        DeliveryResult result = DeliveryResult.failure("orders", "corr-2",
                ErrorCode.DELIVERY_TIMEOUT, "timed out");
        emitter.recordResult(result, Duration.ofMillis(5000));

        double count = registry.counter(
                "kafka_sdk_messages_produced_total",
                "topic", "orders",
                "cluster", "test-cluster",
                "outcome", "delivery_timeout").count();

        assertThat(count).isEqualTo(1.0);
    }

    @Test
    void recordResult_incrementsSchemaRejectedCounter() {
        DeliveryResult result = DeliveryResult.failure("orders", "corr-3",
                ErrorCode.SCHEMA_VALIDATION_FAILED, "invalid schema");
        emitter.recordResult(result, Duration.ofMillis(1));

        double schemaCount = registry.counter(
                "kafka_sdk_messages_schema_rejected_total",
                "topic", "orders").count();

        assertThat(schemaCount).isEqualTo(1.0);
    }

    @Test
    void recordResult_recordsLatencyTimer() {
        DeliveryResult result = DeliveryResult.success("orders", "corr-4", 0, 1L);
        emitter.recordResult(result, Duration.ofMillis(8));

        double totalTime = registry.timer(
                "kafka_sdk_produce_latency_seconds",
                "topic", "orders",
                "cluster", "test-cluster").totalTime(java.util.concurrent.TimeUnit.MILLISECONDS);

        assertThat(totalTime).isEqualTo(8.0);
    }
}
