package com.kafka.sdk.schema;

import com.kafka.sdk.model.exception.SchemaValidationException;

/** SPI for schema validation. Validates a message payload before it is sent to Kafka. */
public interface SchemaValidator {

    /**
     * Validates the payload against the schema registered for the given topic.
     *
     * @param topic   the target Kafka topic (used to resolve the schema subject)
     * @param payload the message payload to validate
     * @throws SchemaValidationException if the payload does not conform to the registered schema
     */
    void validate(String topic, Object payload) throws SchemaValidationException;
}
