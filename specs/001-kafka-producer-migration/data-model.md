# Data Model: Kafka Producer Migration SDK

**Phase**: 1 — Design
**Date**: 2026-05-06
**Feature**: `specs/001-kafka-producer-migration`

## Entities

### ClusterConfig

Immutable value object describing a target Kafka cluster. Loaded once at SDK initialization.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | String | Yes | Human-readable identifier for this cluster (used in logs/metrics) |
| `type` | ClusterType | Yes | Enum: `MSK`, `STRIMZI_MTLS`, `STRIMZI_SCRAM` |
| `bootstrapServers` | String | Yes | Comma-separated list of `host:port` broker addresses |
| `authConfig` | AuthConfig | Yes | Authentication configuration (type-specific) |
| `schemaRegistryConfig` | SchemaRegistryConfig | Yes | Schema Registry connectivity |

**Validation rules**:
- `bootstrapServers` MUST be non-empty and contain at least one valid `host:port` entry
- `type` and `authConfig.mechanism` MUST be consistent (e.g., `MSK` requires `IAM` mechanism)
- All required fields MUST be non-null; missing fields throw `ConfigurationException` at `build()`

---

### AuthConfig

Immutable value object carrying authentication parameters. Fields required depend on `mechanism`.

| Field | Type | Required for | Description |
|-------|------|-------------|-------------|
| `mechanism` | AuthMechanism | Always | Enum: `IAM`, `MTLS`, `SCRAM_SHA_512` |
| `tlsKeystorePath` | String | MTLS | Filesystem path to the client keystore (JKS or PKCS12) |
| `tlsKeystorePassword` | char[] | MTLS | Keystore password (char[] to allow zeroing after use) |
| `tlsTruststorePath` | String | MTLS | Filesystem path to the CA truststore |
| `tlsTruststorePassword` | char[] | MTLS | Truststore password |
| `scramUsername` | String | SCRAM | SCRAM username |
| `scramPassword` | char[] | SCRAM | SCRAM password (char[] to allow zeroing after use) |

**Validation rules**:
- For `MTLS`: `tlsKeystorePath`, `tlsKeystorePassword`, `tlsTruststorePath` MUST be non-null
- For `SCRAM_SHA_512`: `scramUsername` and `scramPassword` MUST be non-null
- For `IAM`: No additional fields required (credentials sourced from EC2 instance profile / IRSA)
- Password fields MUST NOT be included in `toString()` output

---

### SchemaRegistryConfig

Immutable value object for Confluent Schema Registry connectivity.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `url` | String | Yes | — | Schema Registry base URL (e.g., `http://sr.internal:8081`) |
| `cacheMaxSize` | int | No | 100 | Maximum number of schemas held in the in-memory cache |
| `cacheTtlSeconds` | long | No | 600 | Time-to-live for cached schemas in seconds |
| `connectTimeoutMs` | int | No | 3000 | HTTP connect timeout to Schema Registry in milliseconds |
| `readTimeoutMs` | int | No | 5000 | HTTP read timeout for schema fetch in milliseconds |

**Validation rules**:
- `url` MUST be a valid HTTP/HTTPS URL; empty or null throws `ConfigurationException`
- `cacheMaxSize` MUST be > 0
- `cacheTtlSeconds` MUST be > 0

---

### Message

Value object representing a single produce request. Created by the calling application.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `topic` | String | Yes | Target Kafka topic name |
| `key` | byte[] | No | Optional message key (used for partition assignment) |
| `payload` | Object | Yes | Message payload; MUST conform to the registered schema for `topic` |
| `headers` | Map<String,String> | No | Optional message headers (e.g., correlation IDs from upstream) |

**Validation rules**:
- `topic` MUST be non-null and non-empty
- `payload` MUST be non-null
- Header keys and values MUST be non-null strings (null values are silently filtered)

---

### DeliveryResult

Immutable value object returned to the caller after a produce attempt.

| Field | Type | Present when | Description |
|-------|------|-------------|-------------|
| `success` | boolean | Always | `true` if message was delivered; `false` otherwise |
| `topic` | String | Always | Topic the message was (or was not) delivered to |
| `partition` | int | Success only | Partition assigned by the broker |
| `offset` | long | Success only | Offset assigned by the broker |
| `correlationId` | String | Always | UUID linking this delivery to its log/metric/trace entries |
| `errorCode` | ErrorCode | Failure only | Enum: `SCHEMA_VALIDATION_FAILED`, `AUTH_FAILED`, `TOPIC_NOT_FOUND`, `DELIVERY_TIMEOUT`, `UNKNOWN` |
| `errorMessage` | String | Failure only | Human-readable failure description |

---

### SchemaDefinition

Internal cached representation of a schema fetched from the Schema Registry. Not exposed in the
public API.

| Field | Type | Description |
|-------|------|-------------|
| `subject` | String | SR subject name (e.g., `orders-value`) |
| `version` | int | Schema version number |
| `schemaId` | int | SR global schema ID |
| `parsedSchema` | ParsedSchema | Compiled schema object (Avro `Schema`, etc.) |
| `cachedAt` | Instant | Timestamp when this entry was cached (used for TTL eviction) |

---

### ObservabilityContext

Internal per-produce-call container for telemetry. Not exposed in the public API.

| Field | Type | Description |
|-------|------|-------------|
| `correlationId` | String | UUID generated at `produce()` entry; propagated to all signals |
| `startTime` | Instant | Timestamp when `produce()` was called |
| `otelSpan` | Span | OpenTelemetry root span for this produce call |
| `mdcSnapshot` | Map<String,String> | MDC state captured before produce; restored on exit |

---

## Enumerations

### ClusterType

| Value | Description |
|-------|-------------|
| `MSK` | Amazon MSK with IAM authentication |
| `STRIMZI_MTLS` | Strimzi-managed Kafka with mutual TLS |
| `STRIMZI_SCRAM` | Strimzi-managed Kafka with SCRAM-SHA-512 |

### AuthMechanism

| Value | Description |
|-------|-------------|
| `IAM` | AWS IAM via SASL/OAUTHBEARER (MSK only) |
| `MTLS` | Mutual TLS with client certificate |
| `SCRAM_SHA_512` | SASL/SCRAM-SHA-512 with username/password |

### ErrorCode

| Value | Cause |
|-------|-------|
| `SCHEMA_VALIDATION_FAILED` | Payload does not match the registered schema |
| `AUTH_FAILED` | Authentication to the Kafka cluster was rejected |
| `TOPIC_NOT_FOUND` | The target topic does not exist on the cluster |
| `DELIVERY_TIMEOUT` | Kafka acknowledgment not received within the configured timeout |
| `SCHEMA_REGISTRY_UNREACHABLE` | Schema Registry could not be contacted |
| `UNKNOWN` | An unexpected error occurred |

---

## Entity Relationships

```text
KafkaProducerBuilder
  └─ builds ──► KafkaProducerImpl
                  ├─ holds ──► ClusterConfig
                  │              ├─ contains ──► AuthConfig
                  │              └─ contains ──► SchemaRegistryConfig
                  ├─ uses ──► AuthProvider (resolved from AuthConfig.mechanism)
                  ├─ uses ──► SchemaValidator (ConfluentSchemaValidator + SchemaCache)
                  └─ uses ──► ObservabilityContext (created per produce() call)

produce(Message) ──► DeliveryResult
```

---

## State Transitions

### SDK Lifecycle

```text
[UNINITIALIZED]
      │
      │ KafkaProducerBuilder.build()
      │ (validates config, connects to SR, wires auth)
      ▼
[READY] ──── produce(Message) ──► [DELIVERING] ──► [READY]
      │                                  │
      │                                  └── (error) ──► [READY] (error returned, not thrown)
      │
      │ close()
      ▼
[CLOSED] (no further produce calls accepted; throws IllegalStateException)
```

### Message Lifecycle (within a single produce() call)

```text
[RECEIVED]
    │
    │ topic and payload non-null check
    ▼
[VALIDATED_INPUT]
    │
    │ SchemaValidator.validate(topic, payload)
    ▼
[SCHEMA_VALID] ─── (invalid) ──► [REJECTED] → DeliveryResult(success=false, SCHEMA_VALIDATION_FAILED)
    │
    │ KafkaClientFactory → KafkaProducer.send()
    ▼
[INFLIGHT]
    │
    │ Broker acknowledgment received
    ▼
[DELIVERED] → DeliveryResult(success=true, offset=N)
    │
    └── (error) ──► [FAILED] → DeliveryResult(success=false, errorCode)
```
