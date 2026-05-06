package com.kafka.sdk.config;

/** Identifies the type and authentication model of the target Kafka cluster. */
public enum ClusterType {
    /** Amazon MSK with IAM (SASL/OAUTHBEARER) authentication. */
    MSK,
    /** Strimzi-managed Kafka with mutual TLS client certificate authentication. */
    STRIMZI_MTLS,
    /** Strimzi-managed Kafka with SCRAM-SHA-512 username/password authentication. */
    STRIMZI_SCRAM
}
