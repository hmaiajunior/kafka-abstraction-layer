package com.kafka.sdk.model.exception;

/** Thrown at {@code KafkaProducerBuilder.build()} time for invalid or missing configuration. */
public class ConfigurationException extends KafkaSdkException {

    public ConfigurationException(String message) {
        super(message);
    }

    public ConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}
