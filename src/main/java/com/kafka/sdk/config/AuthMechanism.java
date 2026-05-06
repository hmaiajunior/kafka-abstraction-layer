package com.kafka.sdk.config;

/** Specifies the authentication mechanism used to connect to the Kafka cluster. */
public enum AuthMechanism {
    /** AWS IAM via SASL/OAUTHBEARER — for Amazon MSK. */
    IAM,
    /** Mutual TLS with client certificates — for Strimzi. */
    MTLS,
    /** SASL/SCRAM-SHA-512 with username and password — for Strimzi. */
    SCRAM_SHA_512
}
