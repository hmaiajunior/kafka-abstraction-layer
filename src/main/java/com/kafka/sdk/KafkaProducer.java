package com.kafka.sdk;

import com.kafka.sdk.model.DeliveryResult;
import com.kafka.sdk.model.Message;
import java.util.concurrent.CompletableFuture;

/**
 * Primary SDK entry point for producing messages to a Kafka cluster.
 * Implementations are obtained via {@link KafkaProducerBuilder}.
 */
public interface KafkaProducer extends AutoCloseable {

    /**
     * Produces a message to the configured Kafka cluster.
     * Validates the payload against the registered schema before sending.
     * Both success and failure outcomes are encoded in the returned {@link DeliveryResult}.
     *
     * @param message the message to produce; must not be null
     * @return a CompletableFuture completing with the delivery outcome
     * @throws IllegalArgumentException if message is null
     * @throws IllegalStateException    if the producer has been closed
     */
    CompletableFuture<DeliveryResult> produce(Message message);

    /** Closes the producer and releases all underlying resources. */
    @Override
    void close();
}
