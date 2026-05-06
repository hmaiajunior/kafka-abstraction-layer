package com.kafka.sdk.model.exception;

/** Thrown internally when cluster authentication fails. Translated to DeliveryResult. */
public class AuthenticationException extends KafkaSdkException {

    public AuthenticationException(String message) {
        super(message);
    }

    public AuthenticationException(String message, Throwable cause) {
        super(message, cause);
    }
}
