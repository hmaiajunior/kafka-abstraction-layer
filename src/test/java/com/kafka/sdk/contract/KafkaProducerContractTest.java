package com.kafka.sdk.contract;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import com.kafka.sdk.model.Message;
import com.kafka.sdk.observability.MetricsEmitter;
import com.kafka.sdk.observability.ObservabilityContext;
import com.kafka.sdk.observability.TraceEmitter;
import com.kafka.sdk.producer.KafkaProducerImpl;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Contract tests for the {@code KafkaProducer} interface.
 * These tests verify API-level invariants, not implementation details.
 */
class KafkaProducerContractTest {

    @SuppressWarnings("unchecked")
    private final KafkaProducer<String, byte[]> mockKafka = mock(KafkaProducer.class);
    private com.kafka.sdk.KafkaProducer producer;

    @BeforeEach
    void setUp() {
        doAnswer(inv -> {
            Callback cb = inv.getArgument(1);
            RecordMetadata meta = new RecordMetadata(new TopicPartition("t", 0), 0, 1, 0, 0, 0);
            cb.onCompletion(meta, null);
            return null;
        }).when(mockKafka).send(any(), any());

        TraceEmitter traceEmitter = new TraceEmitter(OpenTelemetry.noop());
        MetricsEmitter metricsEmitter = new MetricsEmitter(new SimpleMeterRegistry(), "test");
        ObservabilityContext obs = new ObservabilityContext(metricsEmitter, traceEmitter);
        producer = new KafkaProducerImpl(mockKafka, null, obs);
    }

    @Test
    void produce_throwsIllegalArgumentExceptionWhenMessageIsNull() {
        assertThatThrownBy(() -> producer.produce(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void produce_throwsIllegalStateExceptionAfterClose() {
        producer.close();
        Message msg = Message.forTopic("t").payload("p").build();
        assertThatThrownBy(() -> producer.produce(msg))
                .isInstanceOf(IllegalStateException.class);
    }
}
