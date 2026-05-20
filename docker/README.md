# Producer Migration Demo — Two Strimzi Clusters

Local stack for exercising the **Kafka Producer Migration SDK** end-to-end against two distinct
Strimzi-based Kafka clusters. The point: prove that a producer migrates from `cluster-a` to
`cluster-b` by changing only environment variables — no source changes, no rebuild.

## What gets stood up

| Service           | Image                                                    | Purpose                                                            | Exposed |
|-------------------|----------------------------------------------------------|--------------------------------------------------------------------|---------|
| `certs`           | `eclipse-temurin:17-jdk-alpine`                          | One-shot: generates CA/keystores/truststores per cluster           | —       |
| `kafka-a`         | `quay.io/strimzi/kafka:latest-kafka-3.7.0`               | Strimzi Kafka, KRaft single-node, SSL + mTLS                       | `9092`  |
| `kafka-b`         | `quay.io/strimzi/kafka:latest-kafka-3.7.0`               | Strimzi Kafka, KRaft single-node, SSL + mTLS                       | `9095`  |
| `schema-registry` | `confluentinc/cp-schema-registry:7.6.0`                  | Confluent SR backed by cluster-a; shared across both clusters      | `8081`  |
| `schema-init`     | `alpine:3.19`                                            | One-shot: registers Avro schema for `demo-events-value`            | —       |
| `kafka-ui`        | `provectuslabs/kafka-ui:v0.7.2`                          | Browse both clusters + the SR                                      | `8080`  |
| `producer`        | `kafka-migration-demo` (multi-stage build of SDK + demo) | `DemoProducer` using the SDK; target picked via `KAFKA_SDK_*` envs | —       |
| `consumer`        | `kafka-migration-demo` (same image, different command)   | `DemoConsumer` using plain `kafka-clients`                         | —       |

Both clusters use their own **independent CA** under `/certs/cluster-a/` and `/certs/cluster-b/`
inside the named volume `kafka-certs`. Material is generated only once — re-running `up` skips it.

## Architecture (data flow)

```
                    ┌─────────────────┐
                    │ schema-registry │ ← stores schemas on kafka-a (_schemas topic)
                    │   :8081 (HTTP)  │
                    └────────▲────────┘
                             │ (SDK fetches schema by `demo-events-value`)
                             │
   ┌──────────┐  KAFKA_SDK_* env vars   ┌────────────────────────────────┐
   │ producer │ ─────────────────────►  │      KafkaProducerBuilder      │
   │  (Java)  │                         │  ClusterType.STRIMZI_MTLS      │
   └──────────┘                         │  MtlsAuthProvider + KafkaClient│
                                        └───────────────┬────────────────┘
                                                        │ mTLS
                              ┌─────────────────────────┼─────────────────────────┐
                              ▼                                                   ▼
                       ┌──────────────┐                                    ┌──────────────┐
                       │   kafka-a    │  ←─── consumer (mTLS) ───→         │   kafka-b    │
                       │ :9092 (SSL)  │                                    │ :9095 (SSL)  │
                       │  Strimzi     │                                    │  Strimzi     │
                       └──────────────┘                                    └──────────────┘
                              ▲                                                   ▲
                              └───── kafka-ui (mTLS, both clusters) ──────────────┘
```

## Project layout added by this setup

```
kafka-abstraction-layer/
├── docker-compose.yml              ← orchestrator
├── .env.example                    ← producer/consumer overrides
├── docker/
│   ├── README.md                   ← this file
│   ├── kafka-a/server.properties   ← KRaft + SSL + mTLS, node 1, port 9092
│   ├── kafka-b/server.properties   ← KRaft + SSL + mTLS, node 1, port 9095
│   ├── scripts/
│   │   ├── gen-certs.sh            ← CA + broker + client material (one CA per cluster)
│   │   ├── start-kafka.sh          ← format KRaft (once) + run broker
│   │   └── register-schema.sh      ← POSTs demo Avro schema to SR
│   └── schemas/demo-event.avsc     ← Avro schema for `demo-events-value`
└── demo/
    ├── pom.xml                     ← depends on the SDK installed locally
    ├── Dockerfile                  ← multi-stage: build SDK → build demo → JRE runtime
    └── src/main/java/com/kafka/demo/
        ├── DemoProducer.java       ← uses com.kafka.sdk.KafkaProducer
        └── DemoConsumer.java       ← uses org.apache.kafka.clients.consumer
```

## How the migration actually works (per the SDK)

1. `DemoProducer.main()` calls `ConfigLoader.fromEnvironment()` — reads only `KAFKA_SDK_*` envs.
   It never sees a Kafka API type and never knows the difference between cluster-a and cluster-b.
2. `KafkaProducerBuilder.build()` performs all validation up-front:
   - asserts mandatory mTLS keystore/truststore paths are set (because `CLUSTER_TYPE=STRIMZI_MTLS`);
   - opens an HTTP connection to Schema Registry and lists subjects (fail-fast if SR is unreachable).
3. On each `produce(Message)` call the SDK:
   - validates the topic's subject exists in SR (uses a local LRU cache after first lookup);
   - opens an mTLS connection to the configured bootstrap and `send()`s the record;
   - emits a `correlationId` header so the consumer can correlate.
4. Migration = restart the producer container with the four `PRODUCER_*` envs flipped to `cluster-b`.

## Run it

### 1. Start the source side and the producer

```bash
docker compose up --build -d
docker compose logs -f producer consumer
```

Expected after ~30–60s:
- `kafka-a` and `kafka-b` healthy
- `schema-init` exits 0
- `producer` prints lines like
  `[cluster-a] produced #0 → partition=… offset=… correlationId=…`
- `consumer` (also on cluster-a by default) prints corresponding receives.

Browse **kafka-ui** at <http://localhost:8080> — both clusters appear; `demo-events` exists on
cluster-a only at this point.

### 2. Migrate the producer to cluster-b

In another terminal:

```bash
PRODUCER_CLUSTER=cluster-b \
PRODUCER_BOOTSTRAP=kafka-b:9095 \
PRODUCER_KEYSTORE=/certs/cluster-b/client-b-keystore.jks \
PRODUCER_TRUSTSTORE=/certs/cluster-b/client-b-truststore.jks \
docker compose up -d producer
```

Compose recreates only the producer container with the new env. Watch the logs:

```bash
docker compose logs -f producer
```

You should see the cluster tag flip:
`[cluster-b] produced #0 → partition=… offset=… correlationId=…`

The `consumer` (still on cluster-a) goes quiet. To follow the migration on the consumer side:

```bash
CONSUMER_CLUSTER=cluster-b \
CONSUMER_BOOTSTRAP=kafka-b:9095 \
CONSUMER_KEYSTORE=/certs/cluster-b/client-b-keystore.jks \
CONSUMER_TRUSTSTORE=/certs/cluster-b/client-b-truststore.jks \
docker compose up -d consumer
```

In kafka-ui, `demo-events` now also appears on cluster-b, and its offsets increase while
cluster-a stays frozen.

### 3. Reset and tear down

```bash
docker compose down -v          # also wipes named volumes (certs + kafka data)
```

## Observing the SDK contracts in action

| Constitution gate (from plan.md)                                          | How to see it locally                                                        |
|---------------------------------------------------------------------------|------------------------------------------------------------------------------|
| SR unreachable at startup → `ConfigurationException`                      | `docker compose stop schema-registry && docker compose up producer` → fails. |
| Invalid subject → `SCHEMA_VALIDATION_FAILED` before any Kafka I/O         | Override `DEMO_TOPIC=unknown-topic` for the producer container.              |
| Zero application code changes for cluster switch                          | The migration step above — only env vars change.                             |
| `correlationId` propagation                                               | `consumer` logs `correlationId=…` per record received.                       |
| Credentials never logged                                                  | `grep -i password $(docker compose logs producer)` returns nothing.          |

## Notes & gotchas

- **Persistent state** — the broker data dirs (`kafka-a-data`, `kafka-b-data`) and the certs
  volume survive `docker compose down` (without `-v`). Use `down -v` to start fresh.
- **Cluster IDs are pinned** in `docker-compose.yml`. If you wipe only one broker's data dir,
  the broker will re-format with the same ID — keep the data dir/volume aligned.
- **Hostname verification is disabled** (`ssl.endpoint.identification.algorithm=`). The certs use
  SANs covering `kafka-a`/`kafka-b`/`localhost`, so you can re-enable verification if you connect
  via those names from inside the compose network.
- **Schema Registry is single-cluster-backed** (it persists schemas on cluster-a). This matches
  reality: SR migration is a separate concern from the producer migration the SDK addresses.
- **Strimzi image** — `quay.io/strimzi/kafka` is the same Kafka binary the Strimzi operator runs
  on Kubernetes. We override its entrypoint with our own start script to drive KRaft directly
  rather than through the operator's wrapper. No Strimzi CRDs are involved at this level.
- **Build time** — the first `up --build` pulls Maven dependencies and builds the SDK + demo
  inside the image. Expect 2–5 minutes on a cold cache; subsequent builds are seconds (Docker
  layer cache + local `~/.m2` inside the build stage).

## Tearing into the SDK from a container

To run an ad-hoc admin command against a cluster using the same mTLS material:

```bash
docker compose exec kafka-a /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-a:9092 \
  --command-config /certs/cluster-a/admin.properties \
  --list
```
