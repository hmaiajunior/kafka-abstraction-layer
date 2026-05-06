package com.kafka.sdk.unit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.kafka.sdk.model.DeliveryResult;
import com.kafka.sdk.model.Message;
import com.kafka.sdk.observability.MetricsEmitter;
import com.kafka.sdk.observability.ObservabilityContext;
import com.kafka.sdk.observability.TraceEmitter;
import com.kafka.sdk.producer.KafkaProducerImpl;
import com.kafka.sdk.schema.SchemaValidator;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.SpanBuilder;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KafkaProducerImplTest {

    @Mock
    private KafkaProducer<String, byte[]> mockKafkaProducer;

    @Mock
    private SchemaValidator mockSchemaValidator;

    private com.kafka.sdk.producer.KafkaProducerImpl producer;

    @BeforeEach
    void setUp() {
        OpenTelemetry noopOtel = OpenTelemetry.noop();
        TraceEmitter traceEmitter = new TraceEmitter(noopOtel);
        MetricsEmitter metricsEmitter = new MetricsEmitter(new SimpleMeterRegistry(), "test-cluster");
        ObservabilityContext observabilityContext = new ObservabilityContext(metricsEmitter, traceEmitter);
        producer = new KafkaProducerImpl(mockKafkaProducer, mockSchemaValidator, observabilityContext);
    }

    @Test
    void produce_throwsIllegalArgumentExceptionForNullMessage() {
        assertThatThrownBy(() -> producer.produce(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("null");
    }

    @Test
    void produce_throwsIllegalStateExceptionAfterClose() {
        producer.close();
        Message msg = Message.forTopic("test").payload("hello").build();
        assertThatThrownBy(() -> producer.produce(msg))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
    }

    @Test
    void produce_returnsSuccessResultWhenKafkaAcknowledges() throws Exception {
        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition("orders", 0), 0, 42, 0, 0, 0);

        doAnswer(invocation -> {
            Callback cb = invocation.getArgument(1);
            cb.onCompletion(metadata, null);
            return null;
        }).when(mockKafkaProducer).send(any(), any());

        Message msg = Message.forTopic("orders").payload("event-payload").build();
        DeliveryResult result = producer.produce(msg).get();

        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getTopic()).isEqualTo("orders");
        assertThat(result.getOffset()).isEqualTo(42L);
        assertThat(result.getPartition()).isEqualTo(0);
        assertThat(result.getCorrelationId()).isNotBlank();
    }

    @Test
    void produce_returnsFailureResultWhenKafkaThrows() throws Exception {
        doAnswer(invocation -> {
            Callback cb = invocation.getArgument(1);
            cb.onCompletion(null, new org.apache.kafka.common.errors.TimeoutException("timed out"));
            return null;
        }).when(mockKafkaProducer).send(any(), any());

        Message msg = Message.forTopic("orders").payload("event-payload").build();
        DeliveryResult result = producer.produce(msg).get();

        assertThat(result.isSuccess()).isFalse();
        assertThat(result.getErrorCode()).isEqualTo(com.kafka.sdk.model.ErrorCode.DELIVERY_TIMEOUT);
    }
}
