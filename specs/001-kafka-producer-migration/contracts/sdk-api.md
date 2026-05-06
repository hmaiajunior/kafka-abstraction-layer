# SDK Public API Contract

**Version**: 1.0.0-SNAPSHOT
**Date**: 2026-05-06
**Feature**: `specs/001-kafka-producer-migration`

This document defines the public API surface of the Kafka Producer Migration SDK. Any change to
this surface constitutes a breaking change and requires a MAJOR version bump.

---

## Core Interface: KafkaProducer

The single entry point for all message production. Applications depend only on this interface.

```java
package com.kafka.sdk;

import com.kafka.sdk.model.DeliveryResult;
import com.kafka.sdk.model.Message;
import java.util.concurrent.CompletableFuture;

public interface KafkaProducer extends AutoCloseable {

    /**
     * Produces a message to the configured Kafka cluster.
     *
     * Validates the message payload against the registered schema before sending.
     * Returns a future that completes with a DeliveryResult on both success and failure
     * (failures are encoded in DeliveryResult, not raised as exceptions, except for
     * programming errors such as null arguments).
     *
     * @param message the message to produce; must not be null
     * @return a CompletableFuture completing with the delivery outcome
     * @throws IllegalArgumentException if message is null
     * @throws IllegalStateException    if the producer has been closed
     */
    CompletableFuture<DeliveryResult> produce(Message message);

    /**
     * Closes the producer and releases all underlying resources.
     * No further produce calls are accepted after close().
     */
    @Override
    void close();
}
```

---

## Builder: KafkaProducerBuilder

Fluent builder that constructs and validates a `KafkaProducer` instance at startup.

```java
package com.kafka.sdk;

import com.kafka.sdk.config.ClusterConfig;
import com.kafka.sdk.exception.ConfigurationException;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.OpenTelemetry;

public final class KafkaProducerBuilder {

    /**
     * Sets the cluster configuration. Required.
     *
     * @throws ConfigurationException (at build() time) if config is invalid or incomplete
     */
    public KafkaProducerBuilder withClusterConfig(ClusterConfig config);

    /**
     * Sets the Micrometer MeterRegistry for metrics export. Optional.
     * If not set, a SimpleMeterRegistry is used (metrics tracked in-memory, not exported).
     */
    public KafkaProducerBuilder withMeterRegistry(MeterRegistry registry);

    /**
     * Sets the OpenTelemetry instance for distributed tracing. Optional.
     * If not set, GlobalOpenTelemetry is used (no-op unless configured externally).
     */
    public KafkaProducerBuilder withOpenTelemetry(OpenTelemetry openTelemetry);

    /**
     * Builds and returns a ready-to-use KafkaProducer.
     *
     * Validates all configuration, verifies Schema Registry connectivity, and
     * initializes the authentication provider. Fails fast on any configuration error.
     *
     * @throws ConfigurationException if required config is missing, invalid, or
     *                                if Schema Registry is unreachable at startup
     */
    public KafkaProducer build();
}
```

---

## Configuration: ClusterConfig

```java
package com.kafka.sdk.config;

public final class ClusterConfig {

    public static Builder builder() { ... }

    public String getName()                          { ... }
    public ClusterType getType()                     { ... }
    public String getBootstrapServers()              { ... }
    public AuthConfig getAuthConfig()                { ... }
    public SchemaRegistryConfig getSchemaRegistryConfig() { ... }

    public static final class Builder {
        public Builder name(String name);
        public Builder type(ClusterType type);
        public Builder bootstrapServers(String bootstrapServers);
        public Builder authConfig(AuthConfig authConfig);
        public Builder schemaRegistryConfig(SchemaRegistryConfig schemaRegistryConfig);
        public ClusterConfig build();
    }
}
```

---

## Configuration: AuthConfig

```java
package com.kafka.sdk.config;

public final class AuthConfig {

    public static Builder builder() { ... }

    public AuthMechanism getMechanism()    { ... }

    // mTLS fields — present only when mechanism == MTLS
    public String getTlsKeystorePath()     { ... }
    public String getTlsTruststorePath()   { ... }

    // SCRAM fields — present only when mechanism == SCRAM_SHA_512
    public String getScramUsername()       { ... }

    // NOTE: password/credential accessors are intentionally absent from the public API.
    // Credentials are consumed internally and never re-exposed.

    public static final class Builder {
        public Builder mechanism(AuthMechanism mechanism);

        // mTLS
        public Builder tlsKeystorePath(String path);
        public Builder tlsKeystorePassword(char[] password);
        public Builder tlsTruststorePath(String path);
        public Builder tlsTruststorePassword(char[] password);

        // SCRAM
        public Builder scramUsername(String username);
        public Builder scramPassword(char[] password);

        public AuthConfig build();
    }
}
```

---

## Configuration: SchemaRegistryConfig

```java
package com.kafka.sdk.config;

public final class SchemaRegistryConfig {

    public static Builder builder() { ... }

    public String getUrl()               { ... }
    public int getCacheMaxSize()         { ... }
    public long getCacheTtlSeconds()     { ... }
    public int getConnectTimeoutMs()     { ... }
    public int getReadTimeoutMs()        { ... }

    public static final class Builder {
        public Builder url(String url);
        public Builder cacheMaxSize(int size);
        public Builder cacheTtlSeconds(long ttl);
        public Builder connectTimeoutMs(int timeout);
        public Builder readTimeoutMs(int timeout);
        public SchemaRegistryConfig build();
    }
}
```

---

## Model: Message

```java
package com.kafka.sdk.model;

import java.util.Map;

public final class Message {

    public static Builder forTopic(String topic) { ... }

    public String getTopic()                      { ... }
    public byte[] getKey()                        { ... }  // nullable
    public Object getPayload()                    { ... }
    public Map<String, String> getHeaders()       { ... }  // immutable, never null

    public static final class Builder {
        public Builder key(byte[] key);
        public Builder payload(Object payload);
        public Builder header(String key, String value);
        public Builder headers(Map<String, String> headers);
        public Message build();
    }
}
```

---

## Model: DeliveryResult

```java
package com.kafka.sdk.model;

public final class DeliveryResult {

    public boolean isSuccess()           { ... }
    public String getTopic()             { ... }
    public String getCorrelationId()     { ... }

    // Present only on success
    public int getPartition()            { ... }
    public long getOffset()              { ... }

    // Present only on failure
    public ErrorCode getErrorCode()      { ... }
    public String getErrorMessage()      { ... }
}
```

---

## Enumerations

```java
package com.kafka.sdk.config;

public enum ClusterType {
    MSK,
    STRIMZI_MTLS,
    STRIMZI_SCRAM
}

public enum AuthMechanism {
    IAM,
    MTLS,
    SCRAM_SHA_512
}
```

```java
package com.kafka.sdk.model;

public enum ErrorCode {
    SCHEMA_VALIDATION_FAILED,
    AUTH_FAILED,
    TOPIC_NOT_FOUND,
    DELIVERY_TIMEOUT,
    SCHEMA_REGISTRY_UNREACHABLE,
    UNKNOWN
}
```

---

## Exception Hierarchy

```java
package com.kafka.sdk.exception;

// Base — all SDK exceptions extend this
public class KafkaSdkException extends RuntimeException { ... }

// Thrown at build() time for invalid/missing configuration
public class ConfigurationException extends KafkaSdkException { ... }

// Encoded in DeliveryResult for produce-time failures (NOT thrown to callers)
// Internal use only — surfaced via DeliveryResult.getErrorCode()
public class SchemaValidationException extends KafkaSdkException { ... }
public class AuthenticationException extends KafkaSdkException { ... }
```

**Note**: `SchemaValidationException` and `AuthenticationException` are thrown internally and caught
by `KafkaProducerImpl`. They are translated to `DeliveryResult` with the appropriate `ErrorCode`.
Callers never receive these exceptions directly; only `ConfigurationException` (at `build()` time)
and `IllegalArgumentException` (for null arguments) escape to the caller.

---

## Config Loader: ConfigLoader (Phase 5)

```java
package com.kafka.sdk.config;

public final class ConfigLoader {

    /**
     * Loads ClusterConfig from environment variables.
     * Expected variables:
     *   KAFKA_SDK_CLUSTER_NAME       — cluster name (optional, defaults to "default")
     *   KAFKA_SDK_CLUSTER_TYPE       — MSK | STRIMZI_MTLS | STRIMZI_SCRAM
     *   KAFKA_SDK_BOOTSTRAP_SERVERS  — comma-separated host:port
     *   KAFKA_SDK_SCHEMA_REGISTRY_URL
     *   KAFKA_SDK_AUTH_MECHANISM     — IAM | MTLS | SCRAM_SHA_512
     *   KAFKA_SDK_TLS_KEYSTORE_PATH  — (mTLS only)
     *   KAFKA_SDK_TLS_KEYSTORE_PASS  — (mTLS only)
     *   KAFKA_SDK_TLS_TRUSTSTORE_PATH — (mTLS only)
     *   KAFKA_SDK_TLS_TRUSTSTORE_PASS — (mTLS only)
     *   KAFKA_SDK_SCRAM_USERNAME     — (SCRAM only)
     *   KAFKA_SDK_SCRAM_PASSWORD     — (SCRAM only)
     */
    public static ClusterConfig fromEnvironment();

    /**
     * Loads ClusterConfig from a YAML or .properties file.
     */
    public static ClusterConfig fromFile(String filePath);
}
```

---

## Stability Guarantees

| API Element | Stability |
|-------------|-----------|
| `KafkaProducer` interface | Stable — no breaking changes without MAJOR version bump |
| `KafkaProducerBuilder` | Stable — new `with*` methods may be added (additive only) |
| `Message`, `DeliveryResult` | Stable |
| `ClusterConfig`, `AuthConfig`, `SchemaRegistryConfig` builders | Stable |
| `ClusterType`, `AuthMechanism`, `ErrorCode` enums | Stable — new values may be added |
| `KafkaSdkException` hierarchy | Stable — new subtypes may be added |
| `ConfigLoader` | Beta — env variable names may change before 1.0 GA |
| Internal packages (`producer.*`, `auth.*`, `schema.*`, `observability.*`) | Internal — no stability guarantee |
