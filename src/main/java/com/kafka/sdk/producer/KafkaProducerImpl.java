package com.kafka.sdk.producer;

import com.kafka.sdk.KafkaProducer;
import com.kafka.sdk.model.DeliveryResult;
import com.kafka.sdk.model.ErrorCode;
import com.kafka.sdk.model.Message;
import com.kafka.sdk.model.exception.SchemaValidationException;
import com.kafka.sdk.observability.ObservabilityContext;
import com.kafka.sdk.observability.ObservabilityContext.CallContext;
import com.kafka.sdk.schema.SchemaValidator;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Core implementation of {@link KafkaProducer}.
 * Orchestrates schema validation → Kafka send → observability emission.
 */
public class KafkaProducerImpl implements KafkaProducer {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerImpl.class);

    private final org.apache.kafka.clients.producer.KafkaProducer<String, byte[]> kafkaProducer;
    private final SchemaValidator schemaValidator;
    private final ObservabilityContext observabilityContext;
    private volatile boolean closed = false;

    public KafkaProducerImpl(
            org.apache.kafka.clients.producer.KafkaProducer<String, byte[]> kafkaProducer,
            SchemaValidator schemaValidator,
            ObservabilityContext observabilityContext) {
        this.kafkaProducer = kafkaProducer;
        this.schemaValidator = schemaValidator;
        this.observabilityContext = observabilityContext;
    }

    @Override
    public CompletableFuture<DeliveryResult> produce(Message message) {
        if (message == null) {
            throw new IllegalArgumentException("message must not be null");
        }
        if (closed) {
            throw new IllegalStateException("producer is closed");
        }

        CompletableFuture<DeliveryResult> future = new CompletableFuture<>();
        CallContext ctx = observabilityContext.start(message);

        try {
            if (schemaValidator != null) {
                schemaValidator.validate(message.getTopic(), message.getPayload());
            }

            byte[] value = serializePayload(message.getPayload());
            String key = message.getKey() != null ? new String(message.getKey(), StandardCharsets.UTF_8) : null;

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(message.getTopic(), key, value);
            message.getHeaders().forEach(
                    (k, v) -> record.headers().add(k, v.getBytes(StandardCharsets.UTF_8)));
            // Propagate trace context via headers
            record.headers().add("kafka.sdk.correlationId",
                    ctx.correlationId.getBytes(StandardCharsets.UTF_8));

            kafkaProducer.send(record, (metadata, exception) -> {
                DeliveryResult result;
                if (exception == null) {
                    log.debug("Message delivered: topic={} partition={} offset={} correlationId={}",
                            message.getTopic(), metadata.partition(), metadata.offset(), ctx.correlationId);
                    result = DeliveryResult.success(
                            message.getTopic(), ctx.correlationId,
                            metadata.partition(), metadata.offset());
                } else {
                    log.warn("Message delivery failed: topic={} correlationId={} error={}",
                            message.getTopic(), ctx.correlationId, exception.getMessage());
                    result = DeliveryResult.failure(
                            message.getTopic(), ctx.correlationId,
                            mapException(exception), exception.getMessage());
                }
                observabilityContext.complete(ctx, result);
                future.complete(result);
            });

        } catch (SchemaValidationException e) {
            log.warn("Schema validation rejected message: topic={} correlationId={} reason={}",
                    message.getTopic(), ctx.correlationId, e.getMessage());
            DeliveryResult result = DeliveryResult.failure(
                    message.getTopic(), ctx.correlationId,
                    ErrorCode.SCHEMA_VALIDATION_FAILED, e.getMessage());
            observabilityContext.complete(ctx, result);
            future.complete(result);
        } catch (Exception e) {
            log.error("Unexpected error producing message: topic={} correlationId={}",
                    message.getTopic(), ctx.correlationId, e);
            DeliveryResult result = DeliveryResult.failure(
                    message.getTopic(), ctx.correlationId, ErrorCode.UNKNOWN, e.getMessage());
            observabilityContext.complete(ctx, result);
            future.complete(result);
        }

        return future;
    }

    private byte[] serializePayload(Object payload) {
        if (payload instanceof byte[]) {
            return (byte[]) payload;
        }
        if (payload instanceof String) {
            return ((String) payload).getBytes(StandardCharsets.UTF_8);
        }
        return payload.toString().getBytes(StandardCharsets.UTF_8);
    }

    private ErrorCode mapException(Exception e) {
        if (e instanceof AuthorizationException) {
            return ErrorCode.AUTH_FAILED;
        }
        if (e instanceof UnknownTopicOrPartitionException) {
            return ErrorCode.TOPIC_NOT_FOUND;
        }
        if (e instanceof TimeoutException) {
            return ErrorCode.DELIVERY_TIMEOUT;
        }
        return ErrorCode.UNKNOWN;
    }

    @Override
    public void close() {
        closed = true;
        kafkaProducer.close();
    }
}
