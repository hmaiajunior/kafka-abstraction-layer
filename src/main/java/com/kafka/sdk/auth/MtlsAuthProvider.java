package com.kafka.sdk.auth;

import com.kafka.sdk.config.AuthConfig;
import java.util.Properties;

/**
 * Configures mTLS authentication for Strimzi clusters.
 * Reads keystore and truststore paths from {@link AuthConfig}.
 * Passwords are never written to logs.
 */
public class MtlsAuthProvider implements AuthProvider {

    private final AuthConfig authConfig;

    public MtlsAuthProvider(AuthConfig authConfig) {
        this.authConfig = authConfig;
    }

    @Override
    public Properties toKafkaProperties() {
        Properties props = new Properties();
        props.setProperty("security.protocol", "SSL");
        props.setProperty("ssl.keystore.location", authConfig.getTlsKeystorePath());
        char[] keystorePass = authConfig.getTlsKeystorePassword();
        if (keystorePass != null) {
            props.setProperty("ssl.keystore.password", new String(keystorePass));
        }
        props.setProperty("ssl.truststore.location", authConfig.getTlsTruststorePath());
        char[] truststorePass = authConfig.getTlsTruststorePassword();
        if (truststorePass != null) {
            props.setProperty("ssl.truststore.password", new String(truststorePass));
        }
        return props;
    }

    @Override
    public String toString() {
        return "MtlsAuthProvider{keystorePath=" + authConfig.getTlsKeystorePath()
                + ", truststorePath=" + authConfig.getTlsTruststorePath()
                + ", passwords=[MASKED]}";
    }
}
