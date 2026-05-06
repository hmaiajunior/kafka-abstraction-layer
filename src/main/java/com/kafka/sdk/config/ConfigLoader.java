package com.kafka.sdk.config;

import com.kafka.sdk.model.exception.ConfigurationException;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Loads {@link ClusterConfig} from environment variables or a properties/YAML file.
 * Environment variable prefix: {@code KAFKA_SDK_}.
 */
public final class ConfigLoader {

    private ConfigLoader() {}

    /**
     * Loads cluster configuration from environment variables.
     *
     * <pre>
     * Required:
     *   KAFKA_SDK_CLUSTER_TYPE          — MSK | STRIMZI_MTLS | STRIMZI_SCRAM
     *   KAFKA_SDK_BOOTSTRAP_SERVERS     — comma-separated host:port
     *   KAFKA_SDK_AUTH_MECHANISM        — IAM | MTLS | SCRAM_SHA_512
     *   KAFKA_SDK_SCHEMA_REGISTRY_URL
     *
     * Optional:
     *   KAFKA_SDK_CLUSTER_NAME          — defaults to "default"
     *
     * mTLS only:
     *   KAFKA_SDK_TLS_KEYSTORE_PATH
     *   KAFKA_SDK_TLS_KEYSTORE_PASS
     *   KAFKA_SDK_TLS_TRUSTSTORE_PATH
     *   KAFKA_SDK_TLS_TRUSTSTORE_PASS
     *
     * SCRAM only:
     *   KAFKA_SDK_SCRAM_USERNAME
     *   KAFKA_SDK_SCRAM_PASSWORD
     * </pre>
     */
    public static ClusterConfig fromEnvironment() {
        String clusterType = requireEnv("KAFKA_SDK_CLUSTER_TYPE");
        String bootstrapServers = requireEnv("KAFKA_SDK_BOOTSTRAP_SERVERS");
        String authMechanism = requireEnv("KAFKA_SDK_AUTH_MECHANISM");
        String schemaRegistryUrl = requireEnv("KAFKA_SDK_SCHEMA_REGISTRY_URL");
        String clusterName = System.getenv().getOrDefault("KAFKA_SDK_CLUSTER_NAME", "default");

        ClusterType type;
        try {
            type = ClusterType.valueOf(clusterType.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new ConfigurationException(
                    "Invalid KAFKA_SDK_CLUSTER_TYPE: " + clusterType
                            + ". Valid values: MSK, STRIMZI_MTLS, STRIMZI_SCRAM");
        }

        AuthMechanism mechanism;
        try {
            mechanism = AuthMechanism.valueOf(authMechanism.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new ConfigurationException(
                    "Invalid KAFKA_SDK_AUTH_MECHANISM: " + authMechanism
                            + ". Valid values: IAM, MTLS, SCRAM_SHA_512");
        }

        AuthConfig.Builder authBuilder = AuthConfig.builder().mechanism(mechanism);

        if (mechanism == AuthMechanism.MTLS) {
            authBuilder
                    .tlsKeystorePath(requireEnv("KAFKA_SDK_TLS_KEYSTORE_PATH"))
                    .tlsTruststorePath(requireEnv("KAFKA_SDK_TLS_TRUSTSTORE_PATH"));
            String kp = System.getenv("KAFKA_SDK_TLS_KEYSTORE_PASS");
            String tp = System.getenv("KAFKA_SDK_TLS_TRUSTSTORE_PASS");
            if (kp != null) {
                authBuilder.tlsKeystorePassword(kp.toCharArray());
            }
            if (tp != null) {
                authBuilder.tlsTruststorePassword(tp.toCharArray());
            }
        } else if (mechanism == AuthMechanism.SCRAM_SHA_512) {
            authBuilder.scramUsername(requireEnv("KAFKA_SDK_SCRAM_USERNAME"));
            String sp = System.getenv("KAFKA_SDK_SCRAM_PASSWORD");
            if (sp != null) {
                authBuilder.scramPassword(sp.toCharArray());
            }
        }

        return ClusterConfig.builder()
                .name(clusterName)
                .type(type)
                .bootstrapServers(bootstrapServers)
                .authConfig(authBuilder.build())
                .schemaRegistryConfig(
                        SchemaRegistryConfig.builder().url(schemaRegistryUrl).build())
                .build();
    }

    /**
     * Loads cluster configuration from a {@code .properties} file.
     * Property keys follow the same naming as env vars but lowercase with dots:
     * {@code kafka.sdk.cluster.type}, {@code kafka.sdk.bootstrap.servers}, etc.
     */
    public static ClusterConfig fromFile(String filePath) {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(filePath)) {
            props.load(fis);
        } catch (IOException e) {
            throw new ConfigurationException("Cannot read config file: " + filePath, e);
        }

        String clusterType = requireProp(props, "kafka.sdk.cluster.type");
        String bootstrapServers = requireProp(props, "kafka.sdk.bootstrap.servers");
        String authMechanism = requireProp(props, "kafka.sdk.auth.mechanism");
        String schemaRegistryUrl = requireProp(props, "kafka.sdk.schema.registry.url");
        String clusterName = props.getProperty("kafka.sdk.cluster.name", "default");

        ClusterType type;
        try {
            type = ClusterType.valueOf(clusterType.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new ConfigurationException("Invalid kafka.sdk.cluster.type: " + clusterType);
        }

        AuthMechanism mechanism;
        try {
            mechanism = AuthMechanism.valueOf(authMechanism.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new ConfigurationException("Invalid kafka.sdk.auth.mechanism: " + authMechanism);
        }

        AuthConfig.Builder authBuilder = AuthConfig.builder().mechanism(mechanism);

        if (mechanism == AuthMechanism.MTLS) {
            authBuilder
                    .tlsKeystorePath(requireProp(props, "kafka.sdk.tls.keystore.path"))
                    .tlsTruststorePath(requireProp(props, "kafka.sdk.tls.truststore.path"));
            String kp = props.getProperty("kafka.sdk.tls.keystore.pass");
            String tp = props.getProperty("kafka.sdk.tls.truststore.pass");
            if (kp != null) {
                authBuilder.tlsKeystorePassword(kp.toCharArray());
            }
            if (tp != null) {
                authBuilder.tlsTruststorePassword(tp.toCharArray());
            }
        } else if (mechanism == AuthMechanism.SCRAM_SHA_512) {
            authBuilder.scramUsername(requireProp(props, "kafka.sdk.scram.username"));
            String sp = props.getProperty("kafka.sdk.scram.password");
            if (sp != null) {
                authBuilder.scramPassword(sp.toCharArray());
            }
        }

        return ClusterConfig.builder()
                .name(clusterName)
                .type(type)
                .bootstrapServers(bootstrapServers)
                .authConfig(authBuilder.build())
                .schemaRegistryConfig(
                        SchemaRegistryConfig.builder().url(schemaRegistryUrl).build())
                .build();
    }

    private static String requireEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new ConfigurationException("Required environment variable not set: " + name);
        }
        return value;
    }

    private static String requireProp(Properties props, String key) {
        String value = props.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new ConfigurationException("Required property not set: " + key);
        }
        return value;
    }
}
