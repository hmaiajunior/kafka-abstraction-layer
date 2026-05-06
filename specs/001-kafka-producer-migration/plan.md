# Implementation Plan: Kafka Producer Migration SDK

**Branch**: `001-kafka-producer-migration` | **Date**: 2026-05-06 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `specs/001-kafka-producer-migration/spec.md`

## Summary

The Kafka Producer Migration SDK decouples producer applications from Kafka cluster infrastructure
by providing a stable produce API that abstracts cluster connectivity, authentication, and schema
validation. Applications running on EKS against Amazon MSK (IAM auth) can migrate to Strimzi
(mTLS or SCRAM auth) by changing configuration only — zero code changes. The SDK is implemented
as a Java 17 library embedded directly in each producer application, following an SDK-first approach
with no network proxies, sidecars, or control plane in the MVP phase.

The message flow is:
`App → KafkaProducer.produce() → SchemaValidator → AuthProvider → KafkaClient → Cluster`

## Technical Context

**Language/Version**: Java 17 (LTS)
**Primary Dependencies**: Apache Kafka Clients 3.7.x, Confluent Schema Registry Client 7.x,
AWS MSK IAM Auth 2.x, Micrometer Core 1.12.x, OpenTelemetry Java SDK 1.36.x, SLF4J 2.x
**Storage**: No persistent storage; in-memory LRU cache for schema definitions only
**Testing**: JUnit 5, Mockito 5.x, TestContainers 1.19.x (Kafka + Schema Registry), AssertJ
**Target Platform**: JVM 17+, Linux containers on EKS (Kubernetes)
**Project Type**: Library (SDK — embedded in producer applications as a JAR dependency)
**Performance Goals**: <20ms p99 latency overhead vs. a direct Kafka producer at representative load
**Constraints**: Stateless (no external state store), no hot-reload in MVP, single active cluster
**Scale/Scope**: Per-application embedded library; not a shared service or sidecar

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-checked after Phase 1 design.*

The project constitution is at template state (not yet written to file). The guiding principles
below are derived from the user's constitution input provided at project inception. They will be
validated against the finalized constitution file once `/speckit-constitution` is completed.

| Principle | Status | Evidence |
|-----------|--------|----------|
| SDK-First (no premature proxies/control plane) | ✅ Pass | Embedded JAR; no sidecar, gateway, or proxy |
| Low coupling (app ↔ Kafka infra) | ✅ Pass | App depends only on `KafkaProducer` interface, never on Kafka types |
| Minimal code changes for adoption | ✅ Pass | Zero application source changes required for MSK→Strimzi migration |
| ≤20ms latency budget | ✅ Pass | Local schema cache; no extra network hops in produce hot path |
| Fail-fast on errors | ✅ Pass | `KafkaProducerBuilder.build()` validates all config before first produce |
| No credential exposure | ✅ Pass | Credentials consumed from env/files; absent from produce API and logs |
| Incremental adoption | ✅ Pass | SDK is a drop-in dependency; no infrastructure changes required |

**Gate decision**: All principles satisfied. Proceed to Phase 0.

## Project Structure

### Documentation (this feature)

```text
specs/001-kafka-producer-migration/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/
│   └── sdk-api.md       # Public SDK API contract
└── tasks.md             # Phase 2 output (/speckit-tasks command — NOT created here)
```

### Source Code (repository root)

```text
src/
├── main/java/com/kafka/sdk/
│   ├── KafkaProducer.java                    # Public API interface
│   ├── KafkaProducerBuilder.java             # Fluent builder for constructing producers
│   ├── config/
│   │   ├── ClusterConfig.java                # Cluster connectivity + type (MSK/STRIMZI)
│   │   ├── AuthConfig.java                   # Auth mechanism + credential references
│   │   └── SchemaRegistryConfig.java         # Schema Registry URL + cache settings
│   ├── auth/
│   │   ├── AuthProvider.java                 # SPI interface for auth mechanisms
│   │   ├── MskIamAuthProvider.java           # MSK IAM (SASL/OAUTHBEARER)
│   │   ├── MtlsAuthProvider.java             # Strimzi mTLS (SSL client certs)
│   │   └── ScramAuthProvider.java            # Strimzi SCRAM-SHA-512
│   ├── schema/
│   │   ├── SchemaValidator.java              # SPI interface for schema validation
│   │   ├── ConfluentSchemaValidator.java     # Confluent SR implementation
│   │   └── SchemaCache.java                  # In-memory LRU cache (subject:version → schema)
│   ├── producer/
│   │   ├── KafkaProducerImpl.java            # Internal orchestration
│   │   └── KafkaClientFactory.java           # Creates org.apache.kafka.clients.producer.KafkaProducer
│   ├── observability/
│   │   ├── ObservabilityContext.java         # Correlation ID + trace context per produce call
│   │   ├── MetricsEmitter.java               # Micrometer counters and timers
│   │   └── TraceEmitter.java                 # OpenTelemetry span lifecycle
│   └── model/
│       ├── Message.java                      # Produce request (topic, key?, payload, headers)
│       ├── DeliveryResult.java               # Produce outcome (offset on success, error on failure)
│       └── exception/
│           ├── KafkaSdkException.java        # Base exception
│           ├── SchemaValidationException.java
│           ├── AuthenticationException.java
│           └── ConfigurationException.java
└── test/java/com/kafka/sdk/
    ├── unit/                                 # Pure unit tests (no I/O)
    ├── integration/                          # TestContainers-based tests (Kafka + SR)
    └── contract/                             # Public API contract tests

pom.xml
```

**Structure Decision**: Single Maven project (library). No frontend/backend split. Internal packages
separated by concern (auth, schema, producer, observability) but compiled into a single JAR.
Consumers add a single Maven/Gradle dependency to adopt the SDK.

## Implementation Phases

### Phase 1 — Core SDK + MSK Support

**Goal**: Working produce operation against Amazon MSK with IAM authentication. No schema validation
yet. Establishes the public API surface and internal wiring.

**Deliverables**:
- Maven project skeleton: Java 17, dependency management, checkstyle/spotbugs baseline
- `KafkaProducer` interface with `produce(Message)` returning `CompletableFuture<DeliveryResult>`
- `KafkaProducerBuilder` with mandatory field validation (fail-fast on missing config)
- `ClusterConfig`, `AuthConfig`, `SchemaRegistryConfig` immutable value objects
- `MskIamAuthProvider`: wires `aws-msk-iam-auth` library into Kafka SASL/OAUTHBEARER properties
- `KafkaProducerImpl` and `KafkaClientFactory`: basic produce pipeline (no schema yet)
- `DeliveryResult` and full exception hierarchy
- Unit tests: config validation, MSK auth property generation, produce happy path (mock client)
- Integration test: produce message to Kafka (TestContainers, PLAINTEXT for CI; SASL for staging)

**Constitution gates**:
- `KafkaProducerBuilder.build()` MUST throw `ConfigurationException` on missing required fields

---

### Phase 2 — Schema Registry Integration

**Goal**: Every message validated against Confluent Schema Registry before delivery. Invalid
messages rejected immediately. Schema cache eliminates repeated registry network calls.

**Deliverables**:
- `SchemaValidator` SPI interface: `validate(topic, payload)` throws `SchemaValidationException`
- `ConfluentSchemaValidator`: wraps Confluent SR client, fetches schema by subject (topic-value)
- `SchemaCache`: in-memory LRU cache keyed by `subject:version`; configurable max size and TTL
- Integration of validation in `KafkaProducerImpl.produce()` before any cluster I/O
- SR connectivity check in `KafkaProducerBuilder.build()`
- Unit tests: valid message, invalid message, cache hit, cache miss, SR unreachable at startup
- Integration test: produce with valid and invalid Avro schema to TestContainers SR instance

**Constitution gates**:
- SR unreachable at startup MUST cause `build()` to throw `ConfigurationException` (SC-005)
- Zero SR network calls after first lookup per topic (SC-006)
- Invalid messages MUST be rejected before any Kafka I/O (FR-008)

---

### Phase 3 — Strimzi Support (mTLS + SCRAM)

**Goal**: The same produce API and application code work against a Strimzi-managed Kafka cluster
using both supported authentication mechanisms.

**Deliverables**:
- `MtlsAuthProvider`: reads cert/key/CA from file paths; generates `ssl.*` Kafka client properties
- `ScramAuthProvider`: reads username/password from config; generates `sasl.jaas.config` for SCRAM
- `KafkaClientFactory` updated: selects `AuthProvider` by `ClusterConfig.type` (MSK/STRIMZI_MTLS/STRIMZI_SCRAM)
- Config validation: required fields per cluster type (e.g., cert path mandatory for mTLS)
- Integration tests: produce to Strimzi-flavored Kafka via TestContainers with SSL and SCRAM configs
- Migration test stub: MSK config → Strimzi config swap with same application code

**Constitution gates**:
- Credentials (cert content, passwords) MUST NOT appear in logs at any level (FR-014)

---

### Phase 4 — Observability

**Goal**: Full end-to-end telemetry for every produce operation. Metrics exportable to Prometheus.
Logs structured for parsing. Traces correlated by a single ID across all signals.

**Deliverables**:
- `ObservabilityContext`: generates `correlationId` (UUID) per produce call; holds OTel span context
- `MetricsEmitter` (Micrometer):
  - Counter: `kafka.sdk.messages.produced` (tags: topic, cluster, outcome)
  - Counter: `kafka.sdk.messages.schema_rejected` (tags: topic)
  - Timer: `kafka.sdk.produce.latency` (tags: topic, cluster)
- `TraceEmitter` (OpenTelemetry):
  - Span: `kafka.produce` (parent span, encompasses full operation)
  - Child span: `kafka.schema.validate`
  - Child span: `kafka.client.send`
- SLF4J structured logging with MDC: `correlationId`, `topic`, `cluster`, `outcome`, `offset`
- Unit tests: verify metrics incremented on success/failure, verify spans emitted and named correctly
- Integration test: verify correlationId appears in log output and trace context simultaneously

**Constitution gates**:
- Every produce call MUST emit a correlated span with `correlationId` (SC-004)
- Authentication and schema errors MUST be logged with structured context (FR-011)

---

### Phase 5 — Configuration, Migration Validation & Benchmarks

**Goal**: Prove the migration story end-to-end. Validate latency budget. Provide adoption guide.

**Deliverables**:
- `ConfigLoader`: loads `ClusterConfig` from environment variables and/or YAML/properties files
- Environment variable naming convention: `KAFKA_SDK_CLUSTER_TYPE`, `KAFKA_SDK_BOOTSTRAP_SERVERS`,
  `KAFKA_SDK_AUTH_*`, `KAFKA_SDK_SCHEMA_REGISTRY_URL`
- End-to-end migration test: (1) produce 1,000 messages with MSK config; (2) swap config to
  Strimzi; (3) produce 1,000 messages — assert zero application code changes between steps
- Latency benchmark: JMH microbenchmark comparing `KafkaProducerImpl.produce()` vs. direct
  `org.apache.kafka.clients.producer.KafkaProducer.send()` — assert p99 overhead ≤20ms
- Updated `quickstart.md` with migration walkthrough
- `README.md` at repo root: dependency config, quick example, config reference

**Constitution gates**:
- Zero application code changes for cluster switch MUST be verified by automated test (SC-001)
- p99 latency overhead MUST be ≤20ms as measured by JMH benchmark (SC-002)

## Risks & Trade-offs

| Risk | Impact | Mitigation |
|------|--------|------------|
| IAM token expiry during produce | High — causes auth failures | AWS MSK IAM Auth library refreshes tokens automatically; validate at startup |
| Schema Registry unavailable at startup | High — must fail fast | `build()` validates SR connectivity; configurable grace timeout |
| mTLS cert rotation requires restart | Medium — operational gap | Accepted for MVP; live rotation deferred to future phase |
| Cold-path latency > 20ms (schema fetch) | High — violates SC-002 | LRU cache ensures only first call per topic is cold; budget measured on warm cache |
| TestContainers MSK IAM simulation fidelity | Medium — unit test gap | Use PLAIN auth for unit tests; IAM tested against real MSK in staging/CI |
| Confluent client ↔ SR server version mismatch | Low — serialization errors | Pin Confluent client version to SR server major version in `pom.xml` |
| SCRAM credential leakage via logs | High — security violation | `ScramAuthProvider` masks password in all log output; enforced by test |

## Complexity Tracking

No constitution violations identified. All design decisions are consistent with the SDK-first,
stateless, low-complexity approach mandated by the architectural principles. The single-JAR
library structure is the simplest shape that satisfies all requirements.
