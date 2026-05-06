# Tasks: Kafka Producer Migration SDK

**Input**: Design documents from `specs/001-kafka-producer-migration/`
**Prerequisites**: plan.md ✅ spec.md ✅ research.md ✅ data-model.md ✅ contracts/sdk-api.md ✅
**Scope**: MVP — producers only. Excluded: dual-write, consumers, control plane, advanced routing.

## Format: `[ID] [P?] [Story?] Description`

- **[P]**: Can run in parallel (different files, no dependencies on incomplete tasks)
- **[US#]**: User story label (US1–US4, mapped from spec.md)
- Exact file paths in every description

## Path Conventions

Single Maven project at repository root:

```
src/main/java/com/kafka/sdk/      ← production code
src/test/java/com/kafka/sdk/      ← tests (unit/, integration/, contract/, benchmark/)
pom.xml
```

---

## Phase 1: Setup

**Purpose**: Maven project skeleton, dependency management, and code-quality tooling.

- [x] T001 Create Maven project structure: `pom.xml`, `src/main/java/com/kafka/sdk/`, `src/test/java/com/kafka/sdk/` directories per plan.md layout
- [x] T002 Configure `pom.xml`: Java 17 compiler, kafka-clients 3.7.x, confluent schema-registry-client 7.x, aws-msk-iam-auth 2.x, micrometer-core 1.12.x, opentelemetry-api 1.36.x, slf4j-api 2.x, JUnit 5, Mockito 5.x, TestContainers 1.19.x, AssertJ
- [x] T003 [P] Configure Checkstyle: add `checkstyle.xml` at project root and wire into `pom.xml` build lifecycle
- [x] T004 [P] Configure SpotBugs: add `spotbugs-maven-plugin` to `pom.xml`; set effort=max, threshold=medium

**Checkpoint**: `mvn validate` passes; project compiles with empty source directories.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: All public interfaces, value objects, enums, and exception hierarchy that every user
story implementation depends on. MUST be complete before any user story phase begins.

**⚠️ CRITICAL**: No user story work can begin until this phase is complete.

- [x] T005 Create `KafkaProducer.java` public interface with `produce(Message): CompletableFuture<DeliveryResult>` and `close()` in `src/main/java/com/kafka/sdk/KafkaProducer.java`
- [x] T006 [P] Create `Message.java` immutable value object with inner `Builder` (topic, key, payload, headers) in `src/main/java/com/kafka/sdk/model/Message.java`
- [x] T007 [P] Create `DeliveryResult.java` immutable value object (success, topic, correlationId, partition, offset, errorCode, errorMessage) in `src/main/java/com/kafka/sdk/model/DeliveryResult.java`
- [x] T008 [P] Create `ErrorCode.java` enum (SCHEMA_VALIDATION_FAILED, AUTH_FAILED, TOPIC_NOT_FOUND, DELIVERY_TIMEOUT, SCHEMA_REGISTRY_UNREACHABLE, UNKNOWN) in `src/main/java/com/kafka/sdk/model/ErrorCode.java`
- [x] T009 Create `KafkaSdkException.java` base runtime exception in `src/main/java/com/kafka/sdk/model/exception/KafkaSdkException.java`
- [x] T010 [P] Create `ConfigurationException.java` extending `KafkaSdkException` in `src/main/java/com/kafka/sdk/model/exception/ConfigurationException.java`
- [x] T011 [P] Create `SchemaValidationException.java` extending `KafkaSdkException` in `src/main/java/com/kafka/sdk/model/exception/SchemaValidationException.java`
- [x] T012 [P] Create `AuthenticationException.java` extending `KafkaSdkException` in `src/main/java/com/kafka/sdk/model/exception/AuthenticationException.java`
- [x] T013 Create `ClusterType.java` enum (MSK, STRIMZI_MTLS, STRIMZI_SCRAM) and `AuthMechanism.java` enum (IAM, MTLS, SCRAM_SHA_512) in `src/main/java/com/kafka/sdk/config/`
- [x] T014 Create `ClusterConfig.java` immutable value object with inner `Builder`; validate non-null required fields in `build()` in `src/main/java/com/kafka/sdk/config/ClusterConfig.java`
- [x] T015 [P] Create `AuthConfig.java` immutable value object with inner `Builder`; mask passwords in `toString()` in `src/main/java/com/kafka/sdk/config/AuthConfig.java`
- [x] T016 [P] Create `SchemaRegistryConfig.java` immutable value object with inner `Builder` (url, cacheMaxSize=100, cacheTtlSeconds=600, timeouts) in `src/main/java/com/kafka/sdk/config/SchemaRegistryConfig.java`
- [x] T017 Create `AuthProvider.java` SPI interface with `Properties toKafkaProperties()` in `src/main/java/com/kafka/sdk/auth/AuthProvider.java`
- [x] T018 Create `SchemaValidator.java` SPI interface with `void validate(String topic, Object payload) throws SchemaValidationException` in `src/main/java/com/kafka/sdk/schema/SchemaValidator.java`
- [x] T019 Create `KafkaProducerBuilder.java` skeleton: fields for `ClusterConfig`, `MeterRegistry`, `OpenTelemetry`; `build()` validates required config and throws `ConfigurationException` in `src/main/java/com/kafka/sdk/KafkaProducerBuilder.java`

**Checkpoint**: `mvn compile` passes. All interfaces, models, and exceptions compile cleanly.

---

## Phase 3: User Story 1 — Send Messages Without Cluster Knowledge (Priority: P1) 🎯 MVP

**Goal**: A producer application sends messages via the SDK against Amazon MSK with IAM
authentication without any cluster-specific code in the calling application.

**Independent Test**: Configure SDK with MSK bootstrap servers, call `produce()` with a valid
payload, verify `DeliveryResult.isSuccess() == true` with a non-negative offset. No schema
validation required for this story.

### Implementation for User Story 1

- [x] T020 [US1] Implement `MskIamAuthProvider.java`: return `Properties` with `security.protocol=SASL_SSL`, `sasl.mechanism=AWS_MSK_IAM`, `sasl.jaas.config`, `sasl.client.callback.handler.class` using `aws-msk-iam-auth` library in `src/main/java/com/kafka/sdk/auth/MskIamAuthProvider.java`
- [x] T021 [US1] Implement `KafkaClientFactory.java`: accept `ClusterConfig` + `AuthProvider`, merge Kafka properties, instantiate `org.apache.kafka.clients.producer.KafkaProducer` in `src/main/java/com/kafka/sdk/producer/KafkaClientFactory.java`
- [x] T022 [US1] Implement `KafkaProducerImpl.java`: orchestrate produce flow — `Message` → `KafkaProducer.send()` → `DeliveryResult`; handle `KafkaException` → `DeliveryResult(failure)`; implement `close()` in `src/main/java/com/kafka/sdk/producer/KafkaProducerImpl.java`
- [x] T023 [US1] Complete `KafkaProducerBuilder.build()`: instantiate `MskIamAuthProvider` for `ClusterType.MSK`, wire `KafkaClientFactory`, return `KafkaProducerImpl`; throw `ConfigurationException` for missing `bootstrapServers` or `authConfig` in `src/main/java/com/kafka/sdk/KafkaProducerBuilder.java`
- [x] T024 [P] [US1] Unit test: `ClusterConfig.Builder.build()` throws `ConfigurationException` for missing `bootstrapServers` and missing `authConfig` in `src/test/java/com/kafka/sdk/unit/ClusterConfigTest.java`
- [x] T025 [P] [US1] Unit test: `MskIamAuthProvider.toKafkaProperties()` returns all four required IAM properties with correct values in `src/test/java/com/kafka/sdk/unit/MskIamAuthProviderTest.java`
- [x] T026 [P] [US1] Unit test: `KafkaProducerBuilder.build()` throws `ConfigurationException` when `bootstrapServers` is null or empty in `src/test/java/com/kafka/sdk/unit/KafkaProducerBuilderMissingConfigTest.java`
- [x] T027 [US1] Unit test: `KafkaProducerImpl.produce()` with mocked Kafka client returns success `DeliveryResult` with correct partition and offset in `src/test/java/com/kafka/sdk/unit/KafkaProducerImplTest.java`
- [x] T028 [US1] Contract test: verify `KafkaProducer` interface compliance — `produce(null)` throws `IllegalArgumentException`; `produce()` after `close()` throws `IllegalStateException` in `src/test/java/com/kafka/sdk/contract/KafkaProducerContractTest.java`
- [x] T029 [US1] Integration test: produce a message to Kafka (TestContainers PLAINTEXT cluster, bypassing IAM for CI) and verify `DeliveryResult.isSuccess() == true` in `src/test/java/com/kafka/sdk/integration/CoreProduceIntegrationTest.java`

**Checkpoint**: US1 independently functional. `produce()` delivers to MSK config. p99 latency
baseline recorded for comparison in Phase 7 benchmark.

---

## Phase 4: User Story 2 — Migrate Cluster via Configuration Change (Priority: P1)

**Goal**: An application switches from MSK to Strimzi (mTLS or SCRAM) by changing only
configuration — environment variables or config file — with zero application code changes.

**Independent Test**: Deploy with MSK config → produce messages → swap config to Strimzi (mTLS
or SCRAM) → restart → produce messages → verify identical application source in both runs.

### Implementation for User Story 2

- [x] T030 [US2] Implement `MtlsAuthProvider.java`: read keystore/truststore paths and passwords from `AuthConfig`; return `Properties` with `security.protocol=SSL` and `ssl.*` Kafka properties in `src/main/java/com/kafka/sdk/auth/MtlsAuthProvider.java`
- [x] T031 [US2] Implement `ScramAuthProvider.java`: read username/password from `AuthConfig`; return `Properties` with `security.protocol=SASL_SSL`, `sasl.mechanism=SCRAM-SHA-512`, JAAS config string; mask password in all log output in `src/main/java/com/kafka/sdk/auth/ScramAuthProvider.java`
- [x] T032 [US2] Update `KafkaClientFactory.java`: select `AuthProvider` implementation by `ClusterConfig.type` (MSK → `MskIamAuthProvider`, STRIMZI_MTLS → `MtlsAuthProvider`, STRIMZI_SCRAM → `ScramAuthProvider`) in `src/main/java/com/kafka/sdk/producer/KafkaClientFactory.java`
- [x] T033 [US2] Add cluster-type-specific validation to `KafkaProducerBuilder.build()`: STRIMZI_MTLS requires `tlsKeystorePath`, `tlsTruststorePath`; STRIMZI_SCRAM requires `scramUsername`, `scramPassword`; throw `ConfigurationException` on violation in `src/main/java/com/kafka/sdk/KafkaProducerBuilder.java`
- [x] T034 [US2] Implement `ConfigLoader.fromEnvironment()`: read `KAFKA_SDK_CLUSTER_TYPE`, `KAFKA_SDK_BOOTSTRAP_SERVERS`, `KAFKA_SDK_AUTH_MECHANISM`, `KAFKA_SDK_SCHEMA_REGISTRY_URL`, and auth-specific env vars into `ClusterConfig` in `src/main/java/com/kafka/sdk/config/ConfigLoader.java`
- [x] T035 [P] [US2] Unit test: `MtlsAuthProvider.toKafkaProperties()` returns correct SSL properties; passwords absent from `toString()` in `src/test/java/com/kafka/sdk/unit/MtlsAuthProviderTest.java`
- [x] T036 [P] [US2] Unit test: `ScramAuthProvider.toKafkaProperties()` returns SASL_SSL + SCRAM JAAS config; password masked in `toString()` and in any logged output in `src/test/java/com/kafka/sdk/unit/ScramAuthProviderTest.java`
- [x] T037 [P] [US2] Unit test: `ConfigLoader.fromEnvironment()` correctly parses MSK, STRIMZI_MTLS, and STRIMZI_SCRAM environment variable sets in `src/test/java/com/kafka/sdk/unit/ConfigLoaderTest.java`
- [x] T038 [P] [US2] Unit test: `KafkaProducerBuilder.build()` throws `ConfigurationException` when STRIMZI_MTLS config is missing `tlsKeystorePath` in `src/test/java/com/kafka/sdk/unit/StrimziConfigValidationTest.java`
- [x] T039 [US2] Integration test: produce to Kafka (TestContainers + SCRAM-SHA-512) and verify `DeliveryResult.isSuccess() == true` in `src/test/java/com/kafka/sdk/integration/StrimziScramIntegrationTest.java`
- [x] T040 [US2] Integration test: produce to Kafka (TestContainers + SSL) and verify `DeliveryResult.isSuccess() == true` in `src/test/java/com/kafka/sdk/integration/StrimziMtlsIntegrationTest.java`
- [x] T041 [US2] Integration test (migration): produce 100 messages with MSK config, swap environment variables to Strimzi config (same application class, zero source changes), produce 100 more messages, verify all delivered in `src/test/java/com/kafka/sdk/integration/ClusterMigrationTest.java`

**Checkpoint**: US1 and US2 both independently functional. Cluster migration validated end-to-end
with zero code changes.

---

## Phase 5: User Story 3 — Schema Validation Enforcement (Priority: P2)

**Goal**: Every message is validated against its Confluent Schema Registry schema before any
Kafka I/O. Invalid messages are rejected immediately. Valid schemas are cached to eliminate
repeated registry calls.

**Independent Test**: Attempt to produce a message that violates a registered Avro schema; verify
`DeliveryResult.errorCode == SCHEMA_VALIDATION_FAILED` and that no message reached the Kafka topic.
Produce 1,000 valid messages to the same topic; verify exactly 1 Schema Registry network call made.

### Implementation for User Story 3

- [x] T042 [US3] Implement `SchemaCache.java`: LRU in-memory cache keyed by `subject:version`; configurable `maxSize` and `ttlSeconds`; `get(subject, version)` and `put(subject, version, schema)` methods in `src/main/java/com/kafka/sdk/schema/SchemaCache.java`
- [x] T043 [US3] Implement `ConfluentSchemaValidator.java`: use Confluent `SchemaRegistryClient` to fetch schema by `{topic}-value` subject; validate Avro payload; use `SchemaCache` to skip registry on cache hit; throw `SchemaValidationException` on invalid payload in `src/main/java/com/kafka/sdk/schema/ConfluentSchemaValidator.java`
- [x] T044 [US3] Update `KafkaProducerImpl.produce()`: call `SchemaValidator.validate(topic, payload)` before any call to `KafkaClientFactory`; on `SchemaValidationException`, return `DeliveryResult(failure, SCHEMA_VALIDATION_FAILED)` without producing in `src/main/java/com/kafka/sdk/producer/KafkaProducerImpl.java`
- [x] T045 [US3] Update `KafkaProducerBuilder.build()`: instantiate `ConfluentSchemaValidator` with `SchemaRegistryConfig`; verify SR connectivity (HTTP GET to `/subjects`); throw `ConfigurationException` if SR unreachable in `src/main/java/com/kafka/sdk/KafkaProducerBuilder.java`
- [x] T046 [P] [US3] Unit test: `SchemaCache` — cache hit returns cached schema; TTL expiry evicts entry; LRU evicts oldest entry when `maxSize` exceeded in `src/test/java/com/kafka/sdk/unit/SchemaCacheTest.java`
- [x] T047 [P] [US3] Unit test: `ConfluentSchemaValidator` with mocked `SchemaRegistryClient` — valid payload accepted; invalid payload throws `SchemaValidationException`; second call to same topic hits cache (registry mock called exactly once) in `src/test/java/com/kafka/sdk/unit/ConfluentSchemaValidatorTest.java`
- [x] T048 [P] [US3] Unit test: `KafkaProducerImpl.produce()` with invalid payload — `SchemaValidationException` caught, `DeliveryResult.errorCode == SCHEMA_VALIDATION_FAILED`, mocked Kafka client `send()` never called in `src/test/java/com/kafka/sdk/unit/SchemaRejectionUnitTest.java`
- [x] T049 [US3] Unit test: `KafkaProducerBuilder.build()` throws `ConfigurationException` when Schema Registry URL is unreachable at startup in `src/test/java/com/kafka/sdk/unit/SchemaRegistryConnectivityTest.java`
- [x] T050 [US3] Integration test: produce valid Avro message to Kafka with real TestContainers Schema Registry; verify `DeliveryResult.isSuccess() == true` in `src/test/java/com/kafka/sdk/integration/SchemaValidIntegrationTest.java`
- [x] T051 [US3] Integration test: produce invalid Avro message; verify `DeliveryResult.errorCode == SCHEMA_VALIDATION_FAILED` and zero messages delivered to Kafka topic in `src/test/java/com/kafka/sdk/integration/SchemaInvalidIntegrationTest.java`
- [x] T052 [US3] Integration test: produce 500 messages to same topic; spy on `SchemaRegistryClient`; assert exactly 1 registry network call (cache serves all subsequent validations) in `src/test/java/com/kafka/sdk/integration/SchemaCacheHitIntegrationTest.java`

**Checkpoint**: US3 independently functional. 100% invalid message rejection verified. Schema cache
eliminates repeated registry calls.

---

## Phase 6: User Story 4 — Observe Message Production Flow (Priority: P2)

**Goal**: Every produce operation emits structured metrics (Prometheus), a structured log entry
(SLF4J with MDC), and a distributed trace span (OpenTelemetry). All signals share a single
correlationId enabling end-to-end tracing of any message.

**Independent Test**: Produce one success and one failure (schema rejection); verify the
correlationId from each `DeliveryResult` appears in: (1) the Micrometer counter tag, (2) the
SLF4J MDC log output, (3) the OpenTelemetry span attribute — simultaneously.

### Implementation for User Story 4

- [x] T053 [US4] Implement `ObservabilityContext.java`: generate UUID `correlationId` at produce entry; capture `startTime`; push/pop SLF4J MDC keys (`kafka.sdk.correlationId`, `kafka.sdk.topic`, `kafka.sdk.cluster`, `kafka.sdk.outcome`, `kafka.sdk.offset`); hold OTel `Span` reference in `src/main/java/com/kafka/sdk/observability/ObservabilityContext.java`
- [x] T054 [US4] Implement `MetricsEmitter.java` using Micrometer: counter `kafka_sdk_messages_produced_total` (tags: topic, cluster, outcome); counter `kafka_sdk_messages_schema_rejected_total` (tag: topic); timer `kafka_sdk_produce_latency_seconds` (tags: topic, cluster); counters `kafka_sdk_schema_cache_hits_total` and `kafka_sdk_schema_cache_misses_total` in `src/main/java/com/kafka/sdk/observability/MetricsEmitter.java`
- [x] T055 [US4] Implement `TraceEmitter.java` using OpenTelemetry Java SDK: create root span `kafka.produce`; create child spans `kafka.schema.validate` and `kafka.client.send`; set span attributes (topic, cluster, correlationId, outcome, offset); propagate trace context to Kafka message headers (`traceparent`, `tracestate`) in `src/main/java/com/kafka/sdk/observability/TraceEmitter.java`
- [x] T056 [US4] Integrate observability into `KafkaProducerImpl.produce()`: create `ObservabilityContext` at entry; emit metrics and trace spans for schema validation step and Kafka send step; set MDC keys; ensure context cleaned up in `finally` block in `src/main/java/com/kafka/sdk/producer/KafkaProducerImpl.java`
- [x] T057 [US4] Update `KafkaProducerBuilder`: add `withMeterRegistry(MeterRegistry)` and `withOpenTelemetry(OpenTelemetry)` methods; default to `SimpleMeterRegistry` and `GlobalOpenTelemetry` if not provided in `src/main/java/com/kafka/sdk/KafkaProducerBuilder.java`
- [x] T058 [P] [US4] Unit test: `MetricsEmitter` — `kafka_sdk_messages_produced_total{outcome=success}` increments on success; `{outcome=failure}` increments on delivery error; `kafka_sdk_messages_schema_rejected_total` increments on schema rejection in `src/test/java/com/kafka/sdk/unit/MetricsEmitterTest.java`
- [x] T059 [P] [US4] Unit test: `TraceEmitter` — root span `kafka.produce` created; child spans `kafka.schema.validate` and `kafka.client.send` created; span attributes include `correlationId` and `topic` in `src/test/java/com/kafka/sdk/unit/TraceEmitterTest.java`
- [x] T060 [P] [US4] Unit test: `ObservabilityContext` — `correlationId` is a valid UUID unique per call; MDC keys set during produce and cleared after; `correlationId` matches value in `DeliveryResult` in `src/test/java/com/kafka/sdk/unit/ObservabilityContextTest.java`
- [x] T061 [US4] Integration test: produce one message; capture log output (SLF4J test appender), Micrometer registry, and in-memory OTel exporter; assert same `correlationId` appears in all three signals in `src/test/java/com/kafka/sdk/integration/ObservabilityCorrelationTest.java`

**Checkpoint**: All 4 user stories independently functional. Full telemetry correlated by
`correlationId` across metrics, logs, and traces.

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: Performance validation, security audit, documentation, and final integration check.

- [x] T062 [P] JMH latency benchmark: `LatencyBenchmark.java` — warm-up 5 iterations, measure 10 iterations, compare `KafkaProducerImpl.produce()` vs direct `org.apache.kafka.clients.producer.KafkaProducer.send()` p99 latency; assert SDK overhead ≤20ms in `src/test/java/com/kafka/sdk/benchmark/LatencyBenchmark.java`
- [x] T063 [P] Security audit: verify no credential values (keystore password, SCRAM password, IAM token) appear in log output at any level (DEBUG, INFO, WARN, ERROR) across `MskIamAuthProvider`, `MtlsAuthProvider`, `ScramAuthProvider` in `src/test/java/com/kafka/sdk/unit/CredentialLeakAuditTest.java`
- [x] T064 [P] Javadoc: add method-level Javadoc to all public API classes (`KafkaProducer`, `KafkaProducerBuilder`, `Message`, `DeliveryResult`, `ClusterConfig`, `AuthConfig`, `SchemaRegistryConfig`, `ConfigLoader`)
- [x] T065 Write `README.md` at repository root: Maven dependency snippet, MSK quick start, Strimzi quick start, environment variable reference table, troubleshooting for common `ErrorCode` values
- [x] T066 Validate `quickstart.md` walkthrough: compile and execute each code example against TestContainers; fix any discrepancies between documentation and actual API

**Checkpoint**: All checklist items complete. `mvn verify` passes (including benchmarks and security
audit). README and quickstart validated.

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1 (Setup)**: No dependencies — start immediately
- **Phase 2 (Foundational)**: Depends on Phase 1 — BLOCKS all user story phases
- **Phase 3 (US1 — P1)**: Depends on Phase 2 — No dependencies on other user stories
- **Phase 4 (US2 — P1)**: Depends on Phase 3 (reuses MSK produce pipeline) — Adds Strimzi auth providers
- **Phase 5 (US3 — P2)**: Depends on Phase 3 (schema validation slots into produce pipeline) — Can parallel with Phase 4
- **Phase 6 (US4 — P2)**: Depends on Phase 3 (needs a working produce call to instrument) — Can parallel with Phases 4 and 5
- **Phase 7 (Polish)**: Depends on all user story phases being complete

### User Story Dependencies

- **US1 (P1)**: Can start after Foundational — No story dependencies
- **US2 (P1)**: Depends on US1 (extends produce pipeline with new auth providers)
- **US3 (P2)**: Depends on US1 (inserts schema validation into existing produce pipeline)
- **US4 (P2)**: Depends on US1 (instruments the existing produce pipeline)
- **US3 and US4** can be developed in parallel after US1 completes
- **US2 and US3** can be developed in parallel after US1 completes (different files, no conflict)

### Within Each User Story

- Models → SPI implementations → orchestration layer → builder wiring → tests

---

## Parallel Opportunities

### Phase 2: Run these in parallel (different files)

```
Task T006  Create Message.java
Task T007  Create DeliveryResult.java
Task T008  Create ErrorCode.java
Task T010  Create ConfigurationException.java
Task T011  Create SchemaValidationException.java
Task T012  Create AuthenticationException.java
Task T015  Create AuthConfig.java
Task T016  Create SchemaRegistryConfig.java
```

### Phase 3 (US1): Run tests in parallel after T023

```
Task T024  Unit: ClusterConfigTest
Task T025  Unit: MskIamAuthProviderTest
Task T026  Unit: KafkaProducerBuilderMissingConfigTest
```

### Phase 4 (US2): Run in parallel after T034

```
Task T035  Unit: MtlsAuthProviderTest
Task T036  Unit: ScramAuthProviderTest
Task T037  Unit: ConfigLoaderTest
Task T038  Unit: StrimziConfigValidationTest
Task T039  Integration: StrimziScramIntegrationTest
Task T040  Integration: StrimziMtlsIntegrationTest
```

### Phase 5–6: Run in parallel after US1 (Phase 3) completes

```
Developer A → Phase 5 (US3: Schema Validation — T042 through T052)
Developer B → Phase 6 (US4: Observability — T053 through T061)
```

### Phase 7: Run in parallel after all stories complete

```
Task T062  JMH latency benchmark
Task T063  Security audit
Task T064  Javadoc
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL — blocks all stories)
3. Complete Phase 3: User Story 1 (Core SDK + MSK)
4. **STOP and VALIDATE**: produce works against MSK; `DeliveryResult` returns offset
5. Record p99 baseline latency for Phase 7 benchmark

### Incremental Delivery

1. Setup + Foundational → SDK skeleton compiles, interfaces defined
2. US1 complete → basic produce against MSK works (MVP!)
3. US2 complete → cluster swap MSK ↔ Strimzi validated
4. US3 complete → schema enforcement active; invalid messages blocked
5. US4 complete → full E2E observability correlated by `correlationId`
6. Polish → latency benchmark passes ≤20ms; security audit clean; docs verified

### Excluded from This Task List (Out of Scope — Future Phases)

The following are explicitly excluded per user input and spec scope:
- Dual-write (produce to two clusters simultaneously)
- Consumer migration
- Control plane or routing service
- Advanced routing (content-based, weighted, etc.)
- Hot-reload / in-process cluster switch (restart required in MVP)
- Schema Registry ownership (Confluent SR assumed pre-existing)

---

## Notes

- `[P]` tasks operate on different files — safe to parallelize
- `[US#]` maps each task to its user story for traceability (spec.md)
- Each user story phase delivers an independently testable increment
- TestContainers is used for integration tests; real MSK IAM tested in staging
- SCRAM password and mTLS keystorePassword MUST be `char[]` (zeroed after use) — never `String`
- All `produce()` failures are encoded in `DeliveryResult`; exceptions only escape from `build()`
