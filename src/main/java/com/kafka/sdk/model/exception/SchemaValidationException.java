package com.kafka.sdk.model.exception;

/** Thrown internally when a payload fails schema validation. Translated to DeliveryResult. */
public class SchemaValidationException extends KafkaSdkException {

    public SchemaValidationException(String message) {
        super(message);
    }

    public SchemaValidationException(String message, Throwable cause) {
        super(message, cause);
    }
}
