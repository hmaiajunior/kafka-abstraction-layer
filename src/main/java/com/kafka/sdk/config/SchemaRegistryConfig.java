package com.kafka.sdk.config;

import com.kafka.sdk.model.exception.ConfigurationException;

/** Immutable configuration for the Confluent Schema Registry connection and local cache. */
public final class SchemaRegistryConfig {

    private final String url;
    private final int cacheMaxSize;
    private final long cacheTtlSeconds;
    private final int connectTimeoutMs;
    private final int readTimeoutMs;

    private SchemaRegistryConfig(Builder b) {
        this.url = b.url;
        this.cacheMaxSize = b.cacheMaxSize;
        this.cacheTtlSeconds = b.cacheTtlSeconds;
        this.connectTimeoutMs = b.connectTimeoutMs;
        this.readTimeoutMs = b.readTimeoutMs;
    }

    /** Returns a new builder for {@link SchemaRegistryConfig}. */
    public static Builder builder() {
        return new Builder();
    }

    /** Returns the Schema Registry base URL (e.g., {@code https://sr.example.com}). */
    public String getUrl() { return url; }

    /** Returns the maximum number of schemas held in the local LRU cache. Default: 100. */
    public int getCacheMaxSize() { return cacheMaxSize; }

    /** Returns the time-to-live for cached schemas in seconds. Default: 600 (10 minutes). */
    public long getCacheTtlSeconds() { return cacheTtlSeconds; }

    /** Returns the HTTP connect timeout for Schema Registry requests in milliseconds. Default: 3000. */
    public int getConnectTimeoutMs() { return connectTimeoutMs; }

    /** Returns the HTTP read timeout for Schema Registry requests in milliseconds. Default: 5000. */
    public int getReadTimeoutMs() { return readTimeoutMs; }

    public static final class Builder {
        private String url;
        private int cacheMaxSize = 100;
        private long cacheTtlSeconds = 600;
        private int connectTimeoutMs = 3000;
        private int readTimeoutMs = 5000;

        public Builder url(String url) {
            this.url = url;
            return this;
        }

        public Builder cacheMaxSize(int size) {
            this.cacheMaxSize = size;
            return this;
        }

        public Builder cacheTtlSeconds(long ttl) {
            this.cacheTtlSeconds = ttl;
            return this;
        }

        public Builder connectTimeoutMs(int timeout) {
            this.connectTimeoutMs = timeout;
            return this;
        }

        public Builder readTimeoutMs(int timeout) {
            this.readTimeoutMs = timeout;
            return this;
        }

        public SchemaRegistryConfig build() {
            if (url == null || url.isBlank()) {
                throw new ConfigurationException("schemaRegistryUrl is required");
            }
            if (cacheMaxSize <= 0) {
                throw new ConfigurationException("cacheMaxSize must be > 0");
            }
            if (cacheTtlSeconds <= 0) {
                throw new ConfigurationException("cacheTtlSeconds must be > 0");
            }
            return new SchemaRegistryConfig(this);
        }
    }
}
