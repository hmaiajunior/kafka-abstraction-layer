# Kafka Producer Migration SDK

A Java SDK that enables transparent migration of Kafka producer applications from Amazon MSK (IAM authentication) to Strimzi (mTLS or SCRAM-SHA-512) without changes to application business logic.

## Maven Dependency

```xml
<dependency>
    <groupId>com.kafka</groupId>
    <artifactId>kafka-producer-sdk</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

Confluent packages require the Confluent Maven repository:

```xml
<repository>
    <id>confluent</id>
    <url>https://packages.confluent.io/maven/</url>
</repository>
```

## Quick Start — Amazon MSK (IAM)

```java
ClusterConfig config = ClusterConfig.builder()
    .name("my-msk-cluster")
    .type(ClusterType.MSK)
    .bootstrapServers("b-1.my-cluster.kafka.us-east-1.amazonaws.com:9098")
    .authConfig(AuthConfig.builder().mechanism(AuthMechanism.IAM).build())
    .schemaRegistryConfig(
        SchemaRegistryConfig.builder().url("https://sr.example.com").build())
    .build();

KafkaProducer producer = new KafkaProducerBuilder()
    .withClusterConfig(config)
    .build();

Message msg = Message.forTopic("orders")
    .payload(myAvroRecord)        // GenericRecord | String | byte[]
    .header("source", "checkout")
    .build();

DeliveryResult result = producer.produce(msg).get();

if (result.isSuccess()) {
    System.out.println("offset=" + result.getOffset()
        + " correlationId=" + result.getCorrelationId());
} else {
    System.err.println("failed: " + result.getErrorCode() + " — " + result.getErrorMessage());
}

producer.close();
```

## Quick Start — Strimzi (mTLS)

Only the `ClusterConfig` changes. Application produce code stays identical.

```java
ClusterConfig config = ClusterConfig.builder()
    .name("my-strimzi-cluster")
    .type(ClusterType.STRIMZI_MTLS)
    .bootstrapServers("my-strimzi.kafka.svc.cluster.local:9093")
    .authConfig(AuthConfig.builder()
        .mechanism(AuthMechanism.MTLS)
        .tlsKeystorePath("/certs/client.jks")
        .tlsKeystorePassword("changeit".toCharArray())
        .tlsTruststorePath("/certs/ca.jks")
        .tlsTruststorePassword("changeit".toCharArray())
        .build())
    .schemaRegistryConfig(
        SchemaRegistryConfig.builder().url("https://sr.example.com").build())
    .build();

KafkaProducer producer = new KafkaProducerBuilder()
    .withClusterConfig(config)
    .build();

// produce() call is identical to the MSK example
```

## Quick Start — Strimzi (SCRAM-SHA-512)

```java
ClusterConfig config = ClusterConfig.builder()
    .name("my-strimzi-scram-cluster")
    .type(ClusterType.STRIMZI_SCRAM)
    .bootstrapServers("my-strimzi.kafka.svc.cluster.local:9092")
    .authConfig(AuthConfig.builder()
        .mechanism(AuthMechanism.SCRAM_SHA_512)
        .scramUsername("alice")
        .scramPassword("s3cr3t".toCharArray())
        .build())
    .schemaRegistryConfig(
        SchemaRegistryConfig.builder().url("https://sr.example.com").build())
    .build();

KafkaProducer producer = new KafkaProducerBuilder()
    .withClusterConfig(config)
    .build();
```

## Configuration via Environment Variables

Load configuration without any code change by calling `ConfigLoader.fromEnvironment()`:

```java
ClusterConfig config = ConfigLoader.fromEnvironment();
KafkaProducer producer = new KafkaProducerBuilder().withClusterConfig(config).build();
```

| Variable | Required | Description |
|----------|----------|-------------|
| `KAFKA_SDK_CLUSTER_TYPE` | Yes | `MSK` \| `STRIMZI_MTLS` \| `STRIMZI_SCRAM` |
| `KAFKA_SDK_BOOTSTRAP_SERVERS` | Yes | Comma-separated `host:port` |
| `KAFKA_SDK_AUTH_MECHANISM` | Yes | `IAM` \| `MTLS` \| `SCRAM_SHA_512` |
| `KAFKA_SDK_SCHEMA_REGISTRY_URL` | Yes | Schema Registry base URL |
| `KAFKA_SDK_CLUSTER_NAME` | No | Logical name for metrics/logs (default: `default`) |
| `KAFKA_SDK_TLS_KEYSTORE_PATH` | mTLS only | Path to client keystore (JKS/PKCS12) |
| `KAFKA_SDK_TLS_KEYSTORE_PASS` | mTLS only | Keystore password |
| `KAFKA_SDK_TLS_TRUSTSTORE_PATH` | mTLS only | Path to truststore (JKS/PKCS12) |
| `KAFKA_SDK_TLS_TRUSTSTORE_PASS` | mTLS only | Truststore password |
| `KAFKA_SDK_SCRAM_USERNAME` | SCRAM only | SCRAM username |
| `KAFKA_SDK_SCRAM_PASSWORD` | SCRAM only | SCRAM password |

## Configuration via Properties File

```java
ClusterConfig config = ConfigLoader.fromFile("/etc/kafka-sdk/config.properties");
```

Example `config.properties`:

```properties
kafka.sdk.cluster.type=STRIMZI_SCRAM
kafka.sdk.bootstrap.servers=kafka:9092
kafka.sdk.auth.mechanism=SCRAM_SHA_512
kafka.sdk.schema.registry.url=https://sr.example.com
kafka.sdk.cluster.name=prod-strimzi
kafka.sdk.scram.username=alice
kafka.sdk.scram.password=s3cr3t
```

## Observability

### Metrics (Micrometer / Prometheus)

```java
MeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
KafkaProducer producer = new KafkaProducerBuilder()
    .withClusterConfig(config)
    .withMeterRegistry(registry)
    .build();
```

Emitted metrics:

| Metric | Type | Tags | Description |
|--------|------|------|-------------|
| `kafka_sdk_messages_produced_total` | Counter | `topic`, `cluster`, `outcome` | Total produce attempts |
| `kafka_sdk_messages_schema_rejected_total` | Counter | `topic` | Schema validation rejections |
| `kafka_sdk_produce_latency_seconds` | Timer | `topic`, `cluster` | End-to-end produce latency |
| `kafka_sdk_schema_cache_hits_total` | Counter | — | Schema cache hits |
| `kafka_sdk_schema_cache_misses_total` | Counter | — | Schema cache misses (registry calls) |

### Tracing (OpenTelemetry)

```java
OpenTelemetry otel = GlobalOpenTelemetry.get(); // or your SDK instance
KafkaProducer producer = new KafkaProducerBuilder()
    .withClusterConfig(config)
    .withOpenTelemetry(otel)
    .build();
```

Produced spans:

| Span | Description |
|------|-------------|
| `kafka.produce` | Root span for the full produce pipeline |
| `kafka.schema.validate` | Child span for Schema Registry validation |
| `kafka.client.send` | Child span for the Kafka client send |

All spans carry `kafka.sdk.correlation_id`, `kafka.topic`, and `kafka.sdk.outcome` attributes.

### Structured Logging (SLF4J MDC)

Every produce call sets MDC keys that your logging framework can include in structured log output:

| MDC Key | Value |
|---------|-------|
| `kafka.sdk.correlationId` | UUID correlating logs, metrics, and traces |
| `kafka.sdk.topic` | Destination topic |
| `kafka.sdk.outcome` | `success` or lowercase error code (e.g., `schema_validation_failed`) |
| `kafka.sdk.offset` | Written offset (success only) |

## Troubleshooting

| `ErrorCode` | Cause | Resolution |
|-------------|-------|------------|
| `SCHEMA_VALIDATION_FAILED` | Payload does not match the registered Avro schema, or no schema found for the topic | Register the schema under subject `<topic>-value` in Schema Registry |
| `AUTH_FAILED` | Producer is not authorized to write to the topic | Check IAM role, mTLS certificate, or SCRAM credentials |
| `TOPIC_NOT_FOUND` | Topic does not exist on the cluster | Create the topic or verify bootstrap servers |
| `DELIVERY_TIMEOUT` | Message not acknowledged within the Kafka client timeout | Check network connectivity and broker availability |
| `SCHEMA_REGISTRY_UNREACHABLE` | Schema Registry URL unreachable at startup (caught at `build()` time) | Verify `KAFKA_SDK_SCHEMA_REGISTRY_URL` and network access |
| `UNKNOWN` | Unexpected error during produce | Inspect log output with `correlationId` from `DeliveryResult` |

## Running Tests

```bash
# Unit tests only
mvn test

# Unit + integration tests (requires Docker for TestContainers)
mvn verify
```

## License

Apache License 2.0
