# Quickstart: Kafka Producer Migration SDK

**Date**: 2026-05-06
**Feature**: `specs/001-kafka-producer-migration`

This guide shows how to add the SDK to an existing producer application and how to switch from
Amazon MSK to Strimzi by changing only configuration.

---

## 1. Add the Dependency

**Maven** (`pom.xml`):
```xml
<dependency>
    <groupId>com.kafka</groupId>
    <artifactId>kafka-producer-sdk</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

**Gradle** (`build.gradle`):
```groovy
implementation 'com.kafka:kafka-producer-sdk:1.0.0-SNAPSHOT'
```

---

## 2. Configure for Amazon MSK (IAM Auth)

```java
import com.kafka.sdk.KafkaProducer;
import com.kafka.sdk.KafkaProducerBuilder;
import com.kafka.sdk.config.*;

ClusterConfig mskConfig = ClusterConfig.builder()
    .name("msk-production")
    .type(ClusterType.MSK)
    .bootstrapServers("b-1.msk.us-east-1.amazonaws.com:9098,b-2.msk.us-east-1.amazonaws.com:9098")
    .authConfig(AuthConfig.builder()
        .mechanism(AuthMechanism.IAM)
        .build())
    .schemaRegistryConfig(SchemaRegistryConfig.builder()
        .url("http://schema-registry.internal:8081")
        .build())
    .build();

KafkaProducer producer = new KafkaProducerBuilder()
    .withClusterConfig(mskConfig)
    .build();  // throws ConfigurationException if config is invalid or SR unreachable
```

---

## 3. Produce a Message

```java
import com.kafka.sdk.model.Message;
import com.kafka.sdk.model.DeliveryResult;

// Build the message (payload must match the schema registered for "orders-topic")
Message message = Message.forTopic("orders-topic")
    .payload(orderEvent)          // your domain object; validated against SR schema
    .header("source", "checkout-service")
    .build();

// Produce (async — returns CompletableFuture)
producer.produce(message)
    .thenAccept(result -> {
        if (result.isSuccess()) {
            log.info("Delivered to partition={} offset={} correlationId={}",
                result.getPartition(), result.getOffset(), result.getCorrelationId());
        } else {
            log.error("Delivery failed: {} — {} (correlationId={})",
                result.getErrorCode(), result.getErrorMessage(), result.getCorrelationId());
        }
    });

// Or block if needed:
DeliveryResult result = producer.produce(message).join();
```

---

## 4. Migrate to Strimzi — Change Config Only, Zero Code Changes

To migrate from MSK to Strimzi with mTLS, update **only** the cluster configuration.
The `produce()` call above is identical — no application code changes.

**mTLS variant**:
```java
ClusterConfig strimziConfig = ClusterConfig.builder()
    .name("strimzi-production")
    .type(ClusterType.STRIMZI_MTLS)
    .bootstrapServers("kafka-bootstrap.kafka-namespace.svc.cluster.local:9093")
    .authConfig(AuthConfig.builder()
        .mechanism(AuthMechanism.MTLS)
        .tlsKeystorePath("/etc/kafka/certs/client.keystore.jks")
        .tlsKeystorePassword("changeit".toCharArray())
        .tlsTruststorePath("/etc/kafka/certs/ca.truststore.jks")
        .tlsTruststorePassword("changeit".toCharArray())
        .build())
    .schemaRegistryConfig(SchemaRegistryConfig.builder()
        .url("http://schema-registry.internal:8081")
        .build())
    .build();

KafkaProducer producer = new KafkaProducerBuilder()
    .withClusterConfig(strimziConfig)
    .build();

// Same produce() call as above — no changes needed
```

**SCRAM variant**:
```java
ClusterConfig strimziScramConfig = ClusterConfig.builder()
    .name("strimzi-production")
    .type(ClusterType.STRIMZI_SCRAM)
    .bootstrapServers("kafka-bootstrap.kafka-namespace.svc.cluster.local:9094")
    .authConfig(AuthConfig.builder()
        .mechanism(AuthMechanism.SCRAM_SHA_512)
        .scramUsername(System.getenv("KAFKA_SCRAM_USER"))
        .scramPassword(System.getenv("KAFKA_SCRAM_PASS").toCharArray())
        .build())
    .schemaRegistryConfig(SchemaRegistryConfig.builder()
        .url("http://schema-registry.internal:8081")
        .build())
    .build();
```

---

## 5. Load Config from Environment Variables (Phase 5)

Instead of constructing config in code, use the `ConfigLoader` to read from environment variables.
This allows switching clusters by changing a Kubernetes ConfigMap or secret — no code changes, no
recompilation.

```java
import com.kafka.sdk.config.ConfigLoader;

// Reads all config from KAFKA_SDK_* environment variables
ClusterConfig config = ConfigLoader.fromEnvironment();

KafkaProducer producer = new KafkaProducerBuilder()
    .withClusterConfig(config)
    .build();
```

**MSK environment variables**:
```
KAFKA_SDK_CLUSTER_NAME=msk-production
KAFKA_SDK_CLUSTER_TYPE=MSK
KAFKA_SDK_BOOTSTRAP_SERVERS=b-1.msk.us-east-1.amazonaws.com:9098,...
KAFKA_SDK_AUTH_MECHANISM=IAM
KAFKA_SDK_SCHEMA_REGISTRY_URL=http://schema-registry.internal:8081
```

**Strimzi (mTLS) environment variables**:
```
KAFKA_SDK_CLUSTER_NAME=strimzi-production
KAFKA_SDK_CLUSTER_TYPE=STRIMZI_MTLS
KAFKA_SDK_BOOTSTRAP_SERVERS=kafka-bootstrap.kafka-namespace.svc.cluster.local:9093
KAFKA_SDK_AUTH_MECHANISM=MTLS
KAFKA_SDK_TLS_KEYSTORE_PATH=/etc/kafka/certs/client.keystore.jks
KAFKA_SDK_TLS_KEYSTORE_PASS=<from secret>
KAFKA_SDK_TLS_TRUSTSTORE_PATH=/etc/kafka/certs/ca.truststore.jks
KAFKA_SDK_TLS_TRUSTSTORE_PASS=<from secret>
KAFKA_SDK_SCHEMA_REGISTRY_URL=http://schema-registry.internal:8081
```

---

## 6. Wire Observability (Optional but Recommended)

```java
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.opentelemetry.api.GlobalOpenTelemetry;

KafkaProducer producer = new KafkaProducerBuilder()
    .withClusterConfig(config)
    .withMeterRegistry(new PrometheusMeterRegistry(PrometheusConfig.DEFAULT))
    .withOpenTelemetry(GlobalOpenTelemetry.get())
    .build();
```

**Emitted metrics** (Prometheus format):
```
kafka_sdk_messages_produced_total{topic="orders-topic",cluster="msk-production",outcome="success"}
kafka_sdk_messages_produced_total{topic="orders-topic",cluster="msk-production",outcome="failure"}
kafka_sdk_messages_schema_rejected_total{topic="orders-topic"}
kafka_sdk_produce_latency_seconds_bucket{topic="orders-topic",cluster="msk-production",...}
```

**Correlation via logs** — every `produce()` call sets MDC keys:
```
kafka.sdk.correlationId=550e8400-e29b-41d4-a716-446655440000
kafka.sdk.topic=orders-topic
kafka.sdk.cluster=msk-production
kafka.sdk.outcome=success
kafka.sdk.offset=1042
```

---

## 7. Close the Producer

Always close the producer when the application shuts down to flush pending sends and release
the underlying Kafka client connections.

```java
// With try-with-resources (recommended)
try (KafkaProducer producer = new KafkaProducerBuilder().withClusterConfig(config).build()) {
    producer.produce(message).join();
}

// Or explicitly
producer.close();
```

---

## Common Errors

| Error | Cause | Resolution |
|-------|-------|------------|
| `ConfigurationException: bootstrapServers is required` | Missing bootstrap servers in config | Set `KAFKA_SDK_BOOTSTRAP_SERVERS` or call `.bootstrapServers(...)` |
| `ConfigurationException: Schema Registry unreachable at http://...` | SR not reachable at startup | Verify SR URL and network connectivity before starting the application |
| `DeliveryResult.errorCode = SCHEMA_VALIDATION_FAILED` | Payload does not match registered schema | Ensure payload matches the Avro schema registered for the topic in SR |
| `DeliveryResult.errorCode = AUTH_FAILED` | IAM role missing MSK permissions, cert expired, or wrong SCRAM credentials | Check IAM policy (MSK), certificate validity (mTLS), or credentials (SCRAM) |
| `DeliveryResult.errorCode = TOPIC_NOT_FOUND` | Topic does not exist on the cluster | Create the topic before producing; SDK does not auto-create topics |
