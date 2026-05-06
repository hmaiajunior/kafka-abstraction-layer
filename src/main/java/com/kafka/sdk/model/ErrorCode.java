package com.kafka.sdk.model;

/** Classifies the reason for a failed produce operation. */
public enum ErrorCode {
    SCHEMA_VALIDATION_FAILED,
    AUTH_FAILED,
    TOPIC_NOT_FOUND,
    DELIVERY_TIMEOUT,
    SCHEMA_REGISTRY_UNREACHABLE,
    UNKNOWN
}
