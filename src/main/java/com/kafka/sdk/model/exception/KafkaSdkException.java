package com.kafka.sdk.model.exception;

/** Base exception for all SDK errors. */
public class KafkaSdkException extends RuntimeException {

    public KafkaSdkException(String message) {
        super(message);
    }

    public KafkaSdkException(String message, Throwable cause) {
        super(message, cause);
    }
}
