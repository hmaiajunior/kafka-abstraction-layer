package com.kafka.sdk.observability;

import com.kafka.sdk.model.DeliveryResult;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;

/**
 * Emits Micrometer metrics for every produce operation.
 * Metric names follow Prometheus snake_case convention.
 */
public class MetricsEmitter {

    private static final String PRODUCED = "kafka_sdk_messages_produced_total";
    private static final String SCHEMA_REJECTED = "kafka_sdk_messages_schema_rejected_total";
    private static final String LATENCY = "kafka_sdk_produce_latency_seconds";

    private final MeterRegistry registry;
    private final String clusterName;

    public MetricsEmitter(MeterRegistry registry, String clusterName) {
        this.registry = registry;
        this.clusterName = clusterName;
    }

    public void recordResult(DeliveryResult result, Duration elapsed) {
        String outcome = resolveOutcome(result);
        String topic = result.getTopic();

        Counter.builder(PRODUCED)
                .tag("topic", topic)
                .tag("cluster", clusterName)
                .tag("outcome", outcome)
                .register(registry)
                .increment();

        if ("schema_rejected".equals(outcome)) {
            Counter.builder(SCHEMA_REJECTED)
                    .tag("topic", topic)
                    .register(registry)
                    .increment();
        }

        Timer.builder(LATENCY)
                .tag("topic", topic)
                .tag("cluster", clusterName)
                .register(registry)
                .record(elapsed);
    }

    public void recordCacheHit() {
        Counter.builder("kafka_sdk_schema_cache_hits_total").register(registry).increment();
    }

    public void recordCacheMiss() {
        Counter.builder("kafka_sdk_schema_cache_misses_total").register(registry).increment();
    }

    private String resolveOutcome(DeliveryResult result) {
        if (result.isSuccess()) {
            return "success";
        }
        if (result.getErrorCode() != null) {
            return result.getErrorCode().name().toLowerCase();
        }
        return "failure";
    }
}
