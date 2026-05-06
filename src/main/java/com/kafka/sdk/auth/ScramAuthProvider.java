package com.kafka.sdk.auth;

import com.kafka.sdk.config.AuthConfig;
import java.util.Properties;

/**
 * Configures SASL/SCRAM-SHA-512 authentication for Strimzi clusters.
 * Credentials are consumed from {@link AuthConfig} — never logged.
 */
public class ScramAuthProvider implements AuthProvider {

    private final AuthConfig authConfig;

    public ScramAuthProvider(AuthConfig authConfig) {
        this.authConfig = authConfig;
    }

    @Override
    public Properties toKafkaProperties() {
        Properties props = new Properties();
        props.setProperty("security.protocol", "SASL_SSL");
        props.setProperty("sasl.mechanism", "SCRAM-SHA-512");

        String password = authConfig.getScramPassword() != null
                ? new String(authConfig.getScramPassword())
                : "";
        String jaasConfig = String.format(
                "org.apache.kafka.common.security.scram.ScramLoginModule required"
                        + " username=\"%s\" password=\"%s\";",
                authConfig.getScramUsername(),
                password);
        props.setProperty("sasl.jaas.config", jaasConfig);
        return props;
    }

    @Override
    public String toString() {
        return "ScramAuthProvider{username=" + authConfig.getScramUsername()
                + ", password=[MASKED]}";
    }
}
