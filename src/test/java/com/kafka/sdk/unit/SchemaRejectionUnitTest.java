package com.kafka.sdk.unit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.kafka.sdk.model.DeliveryResult;
import com.kafka.sdk.model.ErrorCode;
import com.kafka.sdk.model.Message;
import com.kafka.sdk.model.exception.SchemaValidationException;
import com.kafka.sdk.observability.MetricsEmitter;
import com.kafka.sdk.observability.ObservabilityContext;
import com.kafka.sdk.observability.TraceEmitter;
import com.kafka.sdk.producer.KafkaProducerImpl;
import com.kafka.sdk.schema.SchemaValidator;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SchemaRejectionUnitTest {

    @Mock
    private KafkaProducer<String, byte[]> mockKafkaProducer;

    @Mock
    private SchemaValidator mockSchemaValidator;

    private KafkaProducerImpl producer;

    @BeforeEach
    void setUp() {
        TraceEmitter traceEmitter = new TraceEmitter(OpenTelemetry.noop());
        MetricsEmitter metricsEmitter = new MetricsEmitter(new SimpleMeterRegistry(), "test");
        ObservabilityContext obs = new ObservabilityContext(metricsEmitter, traceEmitter);
        producer = new KafkaProducerImpl(mockKafkaProducer, mockSchemaValidator, obs);
    }

    @Test
    void produce_returnsSchemaValidationFailedWhenValidatorRejects() throws Exception {
        doThrow(new SchemaValidationException("field 'amount' missing"))
                .when(mockSchemaValidator).validate(eq("orders"), any());

        Message msg = Message.forTopic("orders").payload("bad-payload").build();
        DeliveryResult result = producer.produce(msg).get();

        assertThat(result.isSuccess()).isFalse();
        assertThat(result.getErrorCode()).isEqualTo(ErrorCode.SCHEMA_VALIDATION_FAILED);
        assertThat(result.getErrorMessage()).contains("field 'amount' missing");

        // Kafka send() must NEVER be called when schema validation fails
        verify(mockKafkaProducer, never()).send(any(), any());
    }
}
