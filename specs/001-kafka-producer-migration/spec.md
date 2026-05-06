# Feature Specification: Kafka Producer Migration SDK

**Feature Branch**: `001-kafka-producer-migration`
**Created**: 2026-05-06
**Status**: Draft
**Input**: User description: "Kafka abstraction SDK for seamless producer migration between clusters (MSK to Strimzi)"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Send Messages Without Cluster Knowledge (Priority: P1)

An application developer integrates the SDK and sends messages without needing to know which Kafka
cluster is active, what authentication mechanism is in use, or what bootstrap servers to connect to.
The SDK abstracts all cluster-specific configuration, exposing only a simple produce API.

**Why this priority**: This is the foundational value proposition — if applications can send messages
through a stable SDK interface regardless of underlying cluster, all other migration scenarios become
possible.

**Independent Test**: Can be fully tested by configuring the SDK to point at MSK, calling the produce
API with a valid message, and confirming delivery — no other stories required.

**Acceptance Scenarios**:

1. **Given** an application configured with the SDK pointing to MSK, **When** it calls the produce
   API with a valid message, **Then** the message is delivered to the correct topic on MSK without
   any cluster-specific code in the application.
2. **Given** the SDK is initialized with an invalid cluster address, **When** the application calls
   the produce API, **Then** the SDK returns a clear error immediately (fail-fast) without silently
   dropping the message.
3. **Given** the SDK is initialized with valid configuration, **When** the application produces a
   message, **Then** the total latency overhead introduced by the SDK does not exceed 20ms compared
   to a direct Kafka producer call.

---

### User Story 2 - Migrate Cluster via Configuration Change (Priority: P1)

A platform engineer can switch an application's target Kafka cluster from MSK to Strimzi by updating
only the SDK configuration (environment variables or config file). The application's business logic
and codebase remain completely unchanged.

**Why this priority**: This is the primary migration goal — zero-code-change cluster migration is the
key differentiator and the measure of MVP success.

**Independent Test**: Can be fully tested by deploying an application with SDK configured for MSK,
producing messages, then changing the cluster config to Strimzi, restarting the application, and
confirming messages flow to Strimzi with no code changes.

**Acceptance Scenarios**:

1. **Given** an application running with SDK configured for MSK (IAM auth), **When** the cluster
   configuration is updated to Strimzi (mTLS auth) and the application is restarted, **Then**
   messages are delivered to Strimzi and the application source code is identical in both scenarios.
2. **Given** an application running with SDK configured for MSK, **When** the cluster configuration
   is updated to Strimzi (SCRAM auth) and the application is restarted, **Then** messages are
   delivered to Strimzi without application code changes.
3. **Given** a cluster configuration with missing required fields, **When** the SDK initializes,
   **Then** it fails immediately with a descriptive error identifying the missing fields, before
   attempting to connect.

---

### User Story 3 - Schema Validation Enforcement (Priority: P2)

The SDK validates every message against its registered schema before attempting to send it to the
cluster. Invalid messages are rejected at produce time, preventing malformed data from reaching
downstream consumers.

**Why this priority**: Schema enforcement is a non-negotiable quality gate. Silent schema drift is
one of the most expensive bugs in event-driven systems. This must work correctly for both clusters.

**Independent Test**: Can be fully tested by attempting to produce a message that violates a
registered schema and confirming rejection with a clear validation error, independently of cluster
migration testing.

**Acceptance Scenarios**:

1. **Given** a message that conforms to its registered schema, **When** the produce API is called,
   **Then** the message is accepted and delivered to the cluster.
2. **Given** a message that violates its registered schema, **When** the produce API is called,
   **Then** the SDK rejects the message immediately with a clear validation error and does NOT
   send it to the cluster.
3. **Given** the schema registry is unreachable at SDK initialization, **When** the application
   starts, **Then** the SDK fails fast with a clear connectivity error rather than producing
   unvalidated messages.
4. **Given** a valid schema is cached locally and the registry becomes temporarily unreachable,
   **When** a message is produced, **Then** the SDK uses the cached schema to validate and
   continues to operate without degradation.

---

### User Story 4 - Observe Message Production Flow (Priority: P2)

An operations team can observe the full message production flow — including delivery success/failure,
latency, schema validation results, and cluster connectivity — using their existing monitoring
infrastructure. No custom tooling is required.

**Why this priority**: Observability is essential for validating a migration and for ongoing
operations. Without it, a cluster switch is a blind operation.

**Independent Test**: Can be tested independently by verifying that metrics, structured logs, and
trace data are emitted during message production, covering success and failure scenarios.

**Acceptance Scenarios**:

1. **Given** a message is successfully produced, **When** the operations team queries their
   monitoring system, **Then** they can see delivery confirmation, topic, and end-to-end latency.
2. **Given** a message fails to deliver (authentication error, schema error, network timeout),
   **When** the operations team queries their monitoring system, **Then** they can see the
   failure reason, affected message details, and a correlation ID linking log, metric, and trace.
3. **Given** a cluster migration is in progress, **When** the operations team monitors the system,
   **Then** they can distinguish messages produced to each cluster and track the migration progress.

---

### Edge Cases

- What happens when the schema registry is unreachable at startup?
  → SDK MUST fail fast with a clear error; no messages should be produced unvalidated.
- What happens when Kafka cluster authentication fails (expired IAM role, revoked certificate)?
  → SDK MUST surface an authentication error immediately; no silent retry loops.
- What happens when a message exceeds the maximum allowed size for the cluster?
  → SDK MUST reject the message with a size-exceeded error before attempting delivery.
- What happens when the cluster configuration is valid but the target topic does not exist?
  → SDK MUST surface a topic-not-found error clearly; behavior (create vs. fail) is configurable.
- What happens when the schema registry returns a version mismatch?
  → SDK MUST reject the message and report the schema version conflict explicitly.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The SDK MUST provide a produce API that accepts a topic name, message payload, and
  optional headers, requiring no cluster-specific parameters in the call signature.
- **FR-002**: The SDK MUST abstract cluster connectivity details (bootstrap servers, ports, security
  protocols) into a named cluster configuration loaded at initialization.
- **FR-003**: The SDK MUST support switching the active Kafka cluster by changing configuration
  only, with zero modifications to application source code.
- **FR-004**: The SDK MUST support Amazon MSK as a cluster target, authenticating via IAM
  (AWS SigV4 / SASL/OAUTHBEARER with IAM).
- **FR-005**: The SDK MUST support Strimzi-managed Kafka as a cluster target, authenticating
  via mTLS (mutual TLS with client certificates).
- **FR-006**: The SDK MUST support Strimzi-managed Kafka as a cluster target, authenticating
  via SCRAM-SHA-512 (username/password).
- **FR-007**: The SDK MUST validate every message payload against the schema registered in
  Confluent Schema Registry before transmitting it to the cluster.
- **FR-008**: The SDK MUST reject messages that fail schema validation immediately, returning a
  clear validation error to the caller, without producing to the cluster.
- **FR-009**: The SDK MUST cache schema definitions locally to avoid repeated registry lookups on
  every message production call.
- **FR-010**: The SDK MUST emit structured metrics covering: message production rate, delivery
  success/failure count, end-to-end latency, and schema validation failures.
- **FR-011**: The SDK MUST emit structured logs for every significant event: initialization,
  message produced, validation failure, authentication failure, and cluster connectivity events.
- **FR-012**: The SDK MUST emit distributed trace spans for every message production operation,
  enabling correlation with upstream and downstream services.
- **FR-013**: The SDK MUST fail fast at initialization if required configuration (cluster address,
  authentication credentials, schema registry URL) is missing or invalid.
- **FR-014**: The SDK MUST NOT expose raw cluster credentials (certificates, passwords, IAM tokens)
  to the application layer through the produce API or logs.

### Key Entities *(include if feature involves data)*

- **ProducerConfig**: Named configuration describing a target cluster; includes cluster type
  (MSK/Strimzi), connectivity parameters, and authentication mechanism. Loaded at SDK init.
- **Message**: Unit of data to be produced; carries a topic, payload (schema-validated), optional
  key, and optional headers. The application creates and passes this to the SDK.
- **SchemaDefinition**: The versioned contract for a message's payload structure, fetched from
  Schema Registry and cached locally. Identified by subject and version.
- **DeliveryResult**: The outcome of a produce call returned to the calling application; includes
  success/failure status, offset (on success), and error details (on failure).
- **ObservabilitySpan**: A correlated unit of telemetry (metric sample + log entry + trace span)
  associated with a single produce operation. Carries a correlation ID linking all signals.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A producer application MUST be able to switch from MSK to Strimzi by changing only
  configuration files or environment variables, with zero changes to application source code, as
  verified by a side-by-side code diff showing identical application files before and after migration.
- **SC-002**: The SDK MUST add no more than 20ms of end-to-end latency overhead compared to a
  direct Kafka producer call, measured at the 99th percentile under representative production load.
- **SC-003**: 100% of messages with invalid schemas MUST be rejected before reaching the cluster,
  as verified by producing 1,000 intentionally invalid messages and confirming zero deliveries.
- **SC-004**: Operations teams MUST be able to trace any individual message from produce call to
  cluster acknowledgement using a single correlation ID visible in logs, metrics, and traces.
- **SC-005**: Authentication and schema registry failures MUST surface as explicit errors within
  5 seconds of SDK initialization, with no silent degradation or partial startup.
- **SC-006**: Schema definitions MUST be cached locally such that repeated production calls to
  the same topic incur zero additional schema registry network requests after the first lookup.

## Assumptions

- Applications are deployed on Kubernetes (EKS) and can receive configuration via environment
  variables or mounted config files; no in-process reconfiguration (hot reload) is required.
- The Confluent Schema Registry instance is accessible from all cluster environments (both MSK
  and Strimzi deployments) and maintains consistent schema registrations across environments.
- The IAM role assigned to the EKS workload has the necessary permissions to authenticate with
  MSK; the SDK does not manage IAM role bindings or assume roles itself.
- Client certificates for mTLS authentication with Strimzi are provisioned externally (e.g., via
  cert-manager) and mounted into the application pod; the SDK consumes them from a file path.
- SCRAM credentials for Strimzi are provisioned externally and supplied to the SDK via
  environment variables or a secrets manager; the SDK does not manage credential rotation.
- Only one active cluster target is supported per SDK instance in the MVP; dual-write and
  multi-cluster fan-out are out of scope.
- The SDK targets back-end services (producers) only; consumer migration is explicitly out of scope
  for this MVP phase.
- Topic creation and schema registration are pre-existing operations managed outside the SDK;
  the SDK only produces to existing topics with pre-registered schemas.
