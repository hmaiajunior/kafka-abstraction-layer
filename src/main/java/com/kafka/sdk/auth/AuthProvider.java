package com.kafka.sdk.auth;

import java.util.Properties;

/** SPI for authentication mechanisms. Returns Kafka client properties for the chosen mechanism. */
public interface AuthProvider {

    /**
     * Returns the Kafka client {@link Properties} required to authenticate with the cluster.
     * Implementations MUST NOT include credential values in log output.
     */
    Properties toKafkaProperties();
}
