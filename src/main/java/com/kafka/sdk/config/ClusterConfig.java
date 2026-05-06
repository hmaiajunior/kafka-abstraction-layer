package com.kafka.sdk.config;

import com.kafka.sdk.model.exception.ConfigurationException;

/** Immutable description of a target Kafka cluster. Loaded once at SDK initialization. */
public final class ClusterConfig {

    private final String name;
    private final ClusterType type;
    private final String bootstrapServers;
    private final AuthConfig authConfig;
    private final SchemaRegistryConfig schemaRegistryConfig;

    private ClusterConfig(Builder b) {
        this.name = b.name;
        this.type = b.type;
        this.bootstrapServers = b.bootstrapServers;
        this.authConfig = b.authConfig;
        this.schemaRegistryConfig = b.schemaRegistryConfig;
    }

    /** Returns a new builder for {@link ClusterConfig}. */
    public static Builder builder() {
        return new Builder();
    }

    /** Returns the logical cluster name used in metrics and log output. */
    public String getName() { return name; }

    /** Returns the cluster technology type (MSK, STRIMZI_MTLS, STRIMZI_SCRAM). */
    public ClusterType getType() { return type; }

    /** Returns the Kafka bootstrap servers string (comma-separated host:port pairs). */
    public String getBootstrapServers() { return bootstrapServers; }

    /** Returns the authentication configuration for this cluster. */
    public AuthConfig getAuthConfig() { return authConfig; }

    /** Returns the Schema Registry configuration. */
    public SchemaRegistryConfig getSchemaRegistryConfig() { return schemaRegistryConfig; }

    public static final class Builder {
        private String name = "default";
        private ClusterType type;
        private String bootstrapServers;
        private AuthConfig authConfig;
        private SchemaRegistryConfig schemaRegistryConfig;

        public Builder name(String name) { this.name = name; return this; }
        public Builder type(ClusterType type) { this.type = type; return this; }
        public Builder bootstrapServers(String bs) { this.bootstrapServers = bs; return this; }
        public Builder authConfig(AuthConfig ac) { this.authConfig = ac; return this; }
        public Builder schemaRegistryConfig(SchemaRegistryConfig src) { this.schemaRegistryConfig = src; return this; }

        public ClusterConfig build() {
            if (type == null) {
                throw new ConfigurationException("clusterType is required");
            }
            if (bootstrapServers == null || bootstrapServers.isBlank()) {
                throw new ConfigurationException("bootstrapServers is required");
            }
            if (authConfig == null) {
                throw new ConfigurationException("authConfig is required");
            }
            if (schemaRegistryConfig == null) {
                throw new ConfigurationException("schemaRegistryConfig is required");
            }
            return new ClusterConfig(this);
        }
    }
}
