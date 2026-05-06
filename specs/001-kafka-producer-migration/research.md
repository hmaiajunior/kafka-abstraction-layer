# Research: Kafka Producer Migration SDK

**Phase**: 0 — Research & Decision Log
**Date**: 2026-05-06
**Feature**: `specs/001-kafka-producer-migration`

## Decision 1: Language and Build Tool

**Decision**: Java 17 (LTS) with Maven

**Rationale**: The existing producer applications are Java-based on EKS. Embedding a Java SDK
eliminates any cross-language serialization overhead and allows the SDK to be distributed as a
standard Maven/Gradle dependency. Java 17 is the current LTS release and is supported by all
relevant libraries (Kafka, Confluent, AWS SDK, OTel).

**Alternatives considered**:
- Kotlin: Idiomatic but adds build complexity for teams primarily on Java.
- Java 21: Newer LTS; virtual threads could help async produce, but library compatibility
  is still maturing for the Confluent and AWS clients. Deferred to a future version.
- Gradle: Equivalent to Maven for this use case; Maven chosen for familiarity in most enterprise Java shops.

---

## Decision 2: Kafka Client Library

**Decision**: `org.apache.kafka:kafka-clients:3.7.x` (official Apache Kafka Java client)

**Rationale**: The official client is the reference implementation, widely adopted, and directly
supported by MSK and Strimzi. It supports all required authentication mechanisms (SASL/OAUTHBEARER
for IAM, SSL for mTLS, SASL/SCRAM for SCRAM). The SDK wraps this client behind its own interface,
isolating applications from the Kafka client API entirely.

**Alternatives considered**:
- `reactor-kafka`: Reactive wrapper, not needed — the SDK exposes `CompletableFuture`, not Flux/Mono.
- `vertx-kafka-client`: Vert.x ecosystem, adds runtime dependency that most adopters do not use.

---

## Decision 3: MSK IAM Authentication

**Decision**: `software.amazon.msk:aws-msk-iam-auth:2.x` with SASL/OAUTHBEARER mechanism

**Rationale**: The AWS MSK IAM Auth library implements the `AuthenticateCallbackHandler` interface
required by the Kafka client for IAM-based SASL/OAUTHBEARER. It handles SigV4 token generation and
automatic refresh using the EC2 instance profile or pod IAM role (via IRSA on EKS). The SDK sets
the required Kafka properties (`sasl.mechanism=AWS_MSK_IAM`, `sasl.jaas.config`,
`sasl.client.callback.handler.class`) inside `MskIamAuthProvider` without exposing any AWS SDK
types to the application.

**Alternatives considered**:
- Custom SigV4 implementation: High complexity, maintenance burden, security risk. Rejected.
- MSK SASL/SCRAM: Supported by MSK but not the current production pattern; IAM is the target.

**Key properties set by MskIamAuthProvider**:
```
security.protocol=SASL_SSL
sasl.mechanism=AWS_MSK_IAM
sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
```

---

## Decision 4: Strimzi mTLS Authentication

**Decision**: Native Kafka SSL client configuration using `ssl.*` properties; certificate material
loaded from file paths supplied in `AuthConfig`.

**Rationale**: The Kafka Java client natively supports mTLS via `ssl.keystore.location`,
`ssl.keystore.password`, `ssl.truststore.location`, `ssl.truststore.password`. Certificates are
provisioned by cert-manager and mounted as Kubernetes secrets. The SDK reads paths from config;
it never generates or rotates certificates.

**Key properties set by MtlsAuthProvider**:
```
security.protocol=SSL
ssl.keystore.location=<path>
ssl.keystore.password=<password>
ssl.truststore.location=<path>
ssl.truststore.password=<password>
```

**Credential safety**: keystore/truststore passwords are set as Kafka producer properties directly
and never written to logs. `MtlsAuthProvider.toString()` masks password fields.

---

## Decision 5: Strimzi SCRAM Authentication

**Decision**: SASL/SCRAM-SHA-512 via native Kafka JAAS config; credentials supplied via config
(env vars or config file, never hardcoded).

**Rationale**: Strimzi supports SCRAM-SHA-512 natively. The Kafka client supports it via
`sasl.mechanism=SCRAM-SHA-512` and a JAAS config string. Credentials (username/password) are
supplied to the SDK at initialization from environment variables or a mounted secret. The SDK never
logs credential values.

**Key properties set by ScramAuthProvider**:
```
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required
  username="<username>" password="<password>";
```

---

## Decision 6: Schema Registry Client and Validation

**Decision**: `io.confluent:kafka-schema-registry-client:7.x` with Avro as the default serialization
format; schema subject naming follows the Confluent TopicNameStrategy (`<topic>-value`).

**Rationale**: The Confluent Schema Registry client is the standard library for interacting with
Confluent SR. It provides `SchemaRegistryClient` for fetching schemas and the Avro serializer for
encoding payloads. Validation is performed by deserializing the payload against the fetched schema
before sending; any schema violation raises an `AvroRuntimeException` which the SDK wraps into
`SchemaValidationException`.

**Alternatives considered**:
- JSON Schema: Less commonly used for high-throughput event streams; Avro is industry standard.
- Protobuf: Also supported; can be added in a future phase via the `SchemaValidator` SPI.

---

## Decision 7: Schema Caching Strategy

**Decision**: In-memory LRU cache (`SchemaCache`) keyed by `subject:version`, with configurable
max size (default 100 entries) and TTL (default 10 minutes). Cache populated on first produce to
a topic; subsequent produces within TTL hit cache only.

**Rationale**: The primary latency risk is the schema registry round-trip. Caching eliminates this
from the hot path. LRU with TTL balances memory usage with schema freshness. A 10-minute TTL means
schema changes propagate within minutes in production without manual cache invalidation.

**Alternatives considered**:
- Caffeine cache: Would be ideal but adds a dependency; a simple `LinkedHashMap`-based LRU is
  sufficient for the expected number of topics per application (typically <50).
- No TTL (permanent cache): Risk of stale schema after SR schema evolution. Rejected.
- No cache: Every produce call hits SR network; violates the 20ms latency budget. Rejected.

---

## Decision 8: Metrics

**Decision**: Micrometer Core with a Prometheus-compatible `MeterRegistry`. Applications wire their
own registry (Prometheus, Datadog, CloudWatch) via `KafkaProducerBuilder.withMeterRegistry(...)`.
If none supplied, a `SimpleMeterRegistry` is used (metrics tracked in memory, not exported).

**Rationale**: Micrometer is the de facto metrics facade for Java applications. It decouples the
SDK from any specific metrics backend. EKS applications typically scrape Prometheus; Micrometer's
`PrometheusMeterRegistry` is the expected default.

**Metric names (snake_case, Prometheus convention)**:
- `kafka_sdk_messages_produced_total` (counter, tags: topic, cluster, outcome=success|failure)
- `kafka_sdk_messages_schema_rejected_total` (counter, tags: topic)
- `kafka_sdk_produce_latency_seconds` (timer, tags: topic, cluster)
- `kafka_sdk_schema_cache_hits_total` (counter)
- `kafka_sdk_schema_cache_misses_total` (counter)

---

## Decision 9: Distributed Tracing

**Decision**: OpenTelemetry Java SDK (`io.opentelemetry:opentelemetry-api:1.36.x`) for span
emission. Applications provide an `OpenTelemetry` instance via `KafkaProducerBuilder.withOpenTelemetry(...)`.
If none provided, the SDK uses the GlobalOpenTelemetry instance (no-op by default).

**Rationale**: OpenTelemetry is the vendor-neutral standard. It integrates with Jaeger, Zipkin,
Datadog APM, AWS X-Ray, and Grafana Tempo. The SDK emits spans and propagates trace context via
Kafka message headers (`traceparent`, `tracestate`) so downstream consumers can continue the trace.

**Span hierarchy per produce call**:
```
kafka.produce (root span)
├── kafka.schema.validate
└── kafka.client.send
```

---

## Decision 10: Logging

**Decision**: SLF4J 2.x as the logging facade; no specific binding bundled (applications provide
their own — Logback, Log4j2, etc.). Structured context via MDC keys.

**Rationale**: SLF4J is the standard Java logging facade. Bundling a specific binding would conflict
with adopters' existing logging setup. MDC allows structured key-value pairs to be attached to every
log line produced during a `produce()` call without changing the log format.

**MDC keys set per produce call**:
- `kafka.sdk.correlationId` — UUID for this specific produce call
- `kafka.sdk.topic` — target topic name
- `kafka.sdk.cluster` — cluster identifier from config
- `kafka.sdk.outcome` — `success` | `schema_rejected` | `auth_failure` | `delivery_failure`
- `kafka.sdk.offset` — partition offset (on success only)

---

## Decision 11: Testing Strategy

**Decision**: Three-tier test structure: unit (no I/O), integration (TestContainers), contract
(public API surface).

| Tier | Scope | Tools |
|------|-------|-------|
| Unit | Business logic, config validation, auth property generation | JUnit 5, Mockito |
| Integration | Produce to real Kafka + SR, auth mechanisms end-to-end | TestContainers, Confluent SR image |
| Contract | Public `KafkaProducer` interface stability | JUnit 5 (no mocks — tests interface, not impl) |

**MSK IAM in tests**: AWS MSK IAM authentication cannot be reproduced with TestContainers (requires
real AWS infrastructure). Unit tests mock the auth provider; IAM path is validated in staging
against a real MSK cluster. This is explicitly documented as a test gap in Phase 1.
