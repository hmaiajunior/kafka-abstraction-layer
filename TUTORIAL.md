# Tutorial: Usando o Kafka Producer Migration SDK em Produção

Este tutorial guia você desde zero — infraestrutura AWS + EKS — até uma aplicação Java
produzindo mensagens via MSK e depois migrando para Strimzi sem nenhuma mudança de código.

---

## Sumário

1. [Pré-requisitos](#1-pré-requisitos)
2. [Construir e instalar o SDK](#2-construir-e-instalar-o-sdk)
3. [Criar o cluster Amazon MSK (IAM)](#3-criar-o-cluster-amazon-msk-iam)
4. [Criar o cluster EKS](#4-criar-o-cluster-eks)
5. [Instalar o Strimzi no EKS](#5-instalar-o-strimzi-no-eks)
6. [Criar o cluster Kafka no Strimzi](#6-criar-o-cluster-kafka-no-strimzi)
7. [Implantar o Confluent Schema Registry](#7-implantar-o-confluent-schema-registry)
8. [Registrar um schema Avro](#8-registrar-um-schema-avro)
9. [Criar a aplicação producer](#9-criar-a-aplicação-producer)
10. [Fase 1 — Produzindo para o MSK](#10-fase-1--produzindo-para-o-msk)
11. [Fase 2 — Migrando para o Strimzi (mTLS)](#11-fase-2--migrando-para-o-strimzi-mtls)
12. [Fase 3 — Migrando para o Strimzi (SCRAM)](#12-fase-3--migrando-para-o-strimzi-scram)
13. [Verificar entrega das mensagens](#13-verificar-entrega-das-mensagens)
14. [Observabilidade](#14-observabilidade)
15. [Limpeza de recursos](#15-limpeza-de-recursos)
16. [Solução de problemas comuns](#16-solução-de-problemas-comuns)

---

## 1. Pré-requisitos

### Ferramentas locais

| Ferramenta | Versão mínima | Como instalar |
|------------|--------------|---------------|
| Java JDK | 17 | `sdk install java 17-tem` (SDKMAN) ou OpenJDK |
| Maven | 3.9+ | `sdk install maven` |
| AWS CLI | 2.x | https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html |
| `eksctl` | 0.175+ | `brew install eksctl` / https://eksctl.io |
| `kubectl` | 1.29+ | `brew install kubectl` |
| Helm | 3.x | `brew install helm` |
| `kcat` (kafkacat) | qualquer | `brew install kcat` — para testar tópicos |

### Credenciais AWS

```bash
aws configure
# AWS Access Key ID: <sua-key>
# AWS Secret Access Key: <seu-secret>
# Default region: us-east-1
# Default output format: json
```

Confirme que funciona:

```bash
aws sts get-caller-identity
```

---

## 2. Construir e instalar o SDK

Clone o repositório e instale o SDK no seu repositório Maven local (~/.m2):

```bash
git clone <url-do-repositorio> kafka-producer-sdk
cd kafka-producer-sdk

# Roda apenas testes unitários (sem Docker)
mvn install -DskipITs

# Para rodar todos os testes, incluindo integração (requer Docker)
mvn verify
```

Você deve ver:

```
[INFO] BUILD SUCCESS
[INFO] kafka-producer-sdk 1.0.0-SNAPSHOT installed
```

---

## 3. Criar o cluster Amazon MSK (IAM)

### 3.1 Criar VPC e subnets (se ainda não tiver)

```bash
# Cria uma VPC dedicada para o MSK (opcional — pode usar a VPC padrão)
aws ec2 create-vpc --cidr-block 10.0.0.0/16 --tag-specifications \
  'ResourceType=vpc,Tags=[{Key=Name,Value=kafka-migration-vpc}]'
```

### 3.2 Criar o cluster MSK via AWS CLI

```bash
aws kafka create-cluster \
  --cluster-name "kafka-migration-msk" \
  --kafka-version "3.6.0" \
  --number-of-broker-nodes 2 \
  --broker-node-group-info '{
    "InstanceType": "kafka.m5.large",
    "ClientSubnets": ["<subnet-id-az1>", "<subnet-id-az2>"],
    "SecurityGroups": ["<security-group-id>"],
    "StorageInfo": {"EbsStorageInfo": {"VolumeSize": 100}}
  }' \
  --client-authentication '{
    "Sasl": {"Iam": {"Enabled": true}},
    "Unauthenticated": {"Enabled": false}
  }' \
  --encryption-info '{
    "EncryptionInTransit": {"ClientBroker": "TLS", "InCluster": true}
  }'
```

Aguarde o cluster ficar `ACTIVE` (pode demorar 10–15 minutos):

```bash
aws kafka describe-cluster --cluster-arn <arn-retornado> \
  --query 'ClusterInfo.State'
```

### 3.3 Obter os bootstrap servers

```bash
aws kafka get-bootstrap-brokers --cluster-arn <arn-do-cluster>
# Anote o valor de "BootstrapBrokerStringSaslIam"
```

Exemplo de saída:

```
b-1.kafka-migration.abc123.c3.kafka.us-east-1.amazonaws.com:9098,
b-2.kafka-migration.abc123.c3.kafka.us-east-1.amazonaws.com:9098
```

### 3.4 Criar a IAM policy para o producer

Crie o arquivo `msk-producer-policy.json`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "kafka-cluster:Connect",
        "kafka-cluster:DescribeCluster"
      ],
      "Resource": "arn:aws:kafka:us-east-1:<account-id>:cluster/kafka-migration-msk/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "kafka-cluster:DescribeTopic",
        "kafka-cluster:WriteData",
        "kafka-cluster:ReadData"
      ],
      "Resource": "arn:aws:kafka:us-east-1:<account-id>:topic/kafka-migration-msk/*/*"
    }
  ]
}
```

```bash
aws iam create-policy \
  --policy-name MskProducerPolicy \
  --policy-document file://msk-producer-policy.json
```

### 3.5 Criar o tópico no MSK

```bash
# Instale as Kafka CLI tools ou use um pod temporário
kafka-topics.sh \
  --bootstrap-server <bootstrap-servers-iam> \
  --command-config msk-client.properties \
  --create --topic orders --partitions 3 --replication-factor 2
```

`msk-client.properties`:

```properties
security.protocol=SASL_SSL
sasl.mechanism=AWS_MSK_IAM
sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
```

---

## 4. Criar o cluster EKS

### 4.1 Criar o cluster EKS com eksctl

Crie o arquivo `eks-cluster.yaml`:

```yaml
apiVersion: eksctl.io/v1alpha5
kind: ClusterConfig

metadata:
  name: kafka-migration-eks
  region: us-east-1
  version: "1.29"

managedNodeGroups:
  - name: workers
    instanceType: t3.medium
    minSize: 2
    maxSize: 4
    desiredCapacity: 2
    iam:
      attachPolicyARNs:
        - arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy
        - arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy
        - arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly
        - arn:aws:iam::<account-id>:policy/MskProducerPolicy
```

```bash
eksctl create cluster -f eks-cluster.yaml
# Aguarde ~15 minutos

# Configure o kubectl
aws eks update-kubeconfig --region us-east-1 --name kafka-migration-eks

kubectl get nodes   # deve listar 2 nodes em Ready
```

---

## 5. Instalar o Strimzi no EKS

### 5.1 Adicionar o repositório Helm do Strimzi

```bash
helm repo add strimzi https://strimzi.io/charts/
helm repo update
```

### 5.2 Instalar o operador Strimzi

```bash
kubectl create namespace kafka

helm install strimzi-operator strimzi/strimzi-kafka-operator \
  --namespace kafka \
  --set watchNamespaces="{kafka}"

# Aguarde o operador ficar pronto
kubectl rollout status deployment/strimzi-cluster-operator -n kafka
```

---

## 6. Criar o cluster Kafka no Strimzi

O Strimzi suporta múltiplos tipos de listener. O exemplo abaixo cria um cluster com
**listener mTLS na porta 9093** e **listener SCRAM na porta 9094**.

### 6.1 Criar o Kafka CR

`strimzi-kafka.yaml`:

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: kafka-migration
  namespace: kafka
spec:
  kafka:
    version: 3.6.0
    replicas: 3
    listeners:
      - name: plain
        port: 9092
        type: internal
        tls: false
      - name: tls
        port: 9093
        type: internal
        tls: true
        authentication:
          type: tls               # mTLS — clientes apresentam certificado
      - name: scram
        port: 9094
        type: internal
        tls: true
        authentication:
          type: scram-sha-512
    config:
      offsets.topic.replication.factor: 3
      transaction.state.log.replication.factor: 3
      transaction.state.log.min.isr: 2
      default.replication.factor: 3
      min.insync.replicas: 2
    storage:
      type: jbod
      volumes:
        - id: 0
          type: persistent-claim
          size: 50Gi
          deleteClaim: false
  zookeeper:
    replicas: 3
    storage:
      type: persistent-claim
      size: 10Gi
      deleteClaim: false
  entityOperator:
    topicOperator: {}
    userOperator: {}
```

```bash
kubectl apply -f strimzi-kafka.yaml

# Aguarde o cluster ficar Ready (~5 minutos)
kubectl wait kafka/kafka-migration --for=condition=Ready --timeout=300s -n kafka
```

### 6.2 Criar o tópico no Strimzi

`strimzi-topic.yaml`:

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: orders
  namespace: kafka
  labels:
    strimzi.io/cluster: kafka-migration
spec:
  partitions: 3
  replicas: 3
```

```bash
kubectl apply -f strimzi-topic.yaml
```

### 6.3 Criar o usuário mTLS

`strimzi-user-mtls.yaml`:

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaUser
metadata:
  name: producer-mtls
  namespace: kafka
  labels:
    strimzi.io/cluster: kafka-migration
spec:
  authentication:
    type: tls
  authorization:
    type: simple
    acls:
      - resource:
          type: topic
          name: orders
        operations: [Write, Describe]
      - resource:
          type: cluster
        operations: [Describe]
```

```bash
kubectl apply -f strimzi-user-mtls.yaml
```

### 6.4 Criar o usuário SCRAM

`strimzi-user-scram.yaml`:

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaUser
metadata:
  name: producer-scram
  namespace: kafka
  labels:
    strimzi.io/cluster: kafka-migration
spec:
  authentication:
    type: scram-sha-512
  authorization:
    type: simple
    acls:
      - resource:
          type: topic
          name: orders
        operations: [Write, Describe]
      - resource:
          type: cluster
        operations: [Describe]
```

```bash
kubectl apply -f strimzi-user-scram.yaml
```

### 6.5 Extrair os certificados e credenciais do Strimzi

**Bootstrap servers do Strimzi (internos ao cluster EKS):**

```bash
kubectl get kafka kafka-migration -n kafka \
  -o jsonpath='{.status.listeners[?(@.name=="tls")].bootstrapServers}'
# Saída: kafka-migration-kafka-bootstrap.kafka.svc.cluster.local:9093

kubectl get kafka kafka-migration -n kafka \
  -o jsonpath='{.status.listeners[?(@.name=="scram")].bootstrapServers}'
# Saída: kafka-migration-kafka-bootstrap.kafka.svc.cluster.local:9094
```

**Certificado CA do cluster (para truststore):**

```bash
kubectl get secret kafka-migration-cluster-ca-cert -n kafka \
  -o jsonpath='{.data.ca\.crt}' | base64 -d > ca.crt

# Cria truststore JKS
keytool -import -trustcacerts -alias strimzi-ca \
  -file ca.crt \
  -keystore kafka-truststore.jks \
  -storepass changeit -noprompt
```

**Certificado do cliente mTLS:**

```bash
# Extrai o certificado e a chave privada do usuário mTLS
kubectl get secret producer-mtls -n kafka \
  -o jsonpath='{.data.user\.crt}' | base64 -d > user.crt
kubectl get secret producer-mtls -n kafka \
  -o jsonpath='{.data.user\.key}' | base64 -d > user.key
kubectl get secret producer-mtls -n kafka \
  -o jsonpath='{.data.user\.p12}' | base64 -d > user.p12

# Senha do PKCS12 (gerada pelo Strimzi)
kubectl get secret producer-mtls -n kafka \
  -o jsonpath='{.data.user\.password}' | base64 -d
# Anote essa senha — será usada como KAFKA_SDK_TLS_KEYSTORE_PASS

# Converte para JKS
keytool -importkeystore \
  -srckeystore user.p12 -srcstoretype PKCS12 \
  -destkeystore kafka-keystore.jks -deststoretype JKS \
  -srcstorepass <senha-do-p12> \
  -deststorepass changeit -noprompt

# Copie os arquivos para um local acessível pelo producer
mkdir -p /etc/kafka/certs
cp kafka-keystore.jks kafka-truststore.jks /etc/kafka/certs/
```

**Senha do usuário SCRAM:**

```bash
kubectl get secret producer-scram -n kafka \
  -o jsonpath='{.data.password}' | base64 -d
# Anote essa senha — será KAFKA_SDK_SCRAM_PASSWORD
```

---

## 7. Implantar o Confluent Schema Registry

O Schema Registry precisa apontar para **um** cluster Kafka (MSK ou Strimzi).
Para a migração, o ideal é mantê-lo independente — apontando para o MSK inicialmente
e depois atualizado para o Strimzi.

### 7.1 Implantar no EKS com Helm

```bash
helm repo add confluentinc https://confluentinc.github.io/cp-helm-charts/
helm repo update
```

`schema-registry-values.yaml`:

```yaml
replicaCount: 1

kafka:
  bootstrapServers: "SASL_SSL://b-1.kafka-migration.abc123.c3.kafka.us-east-1.amazonaws.com:9098"

configurationOverrides:
  "kafkastore.security.protocol": "SASL_SSL"
  "kafkastore.sasl.mechanism": "AWS_MSK_IAM"
  "kafkastore.sasl.jaas.config": >-
    software.amazon.msk.auth.iam.IAMLoginModule required;
  "kafkastore.sasl.client.callback.handler.class": >-
    software.amazon.msk.auth.iam.IAMClientCallbackHandler

service:
  type: ClusterIP
  port: 8081
```

```bash
kubectl create namespace schema-registry

helm install schema-registry confluentinc/cp-schema-registry \
  --namespace schema-registry \
  -f schema-registry-values.yaml

kubectl rollout status deployment/schema-registry-cp-schema-registry \
  -n schema-registry
```

**URL interna do Schema Registry:**

```
http://schema-registry-cp-schema-registry.schema-registry.svc.cluster.local:8081
```

Para acessar localmente durante o desenvolvimento:

```bash
kubectl port-forward svc/schema-registry-cp-schema-registry 8081:8081 -n schema-registry &
```

---

## 8. Registrar um schema Avro

Com o port-forward ativo na porta 8081:

### 8.1 Criar o arquivo de schema

`order-schema.json`:

```json
{
  "type": "record",
  "name": "Order",
  "namespace": "com.example.orders",
  "fields": [
    {"name": "orderId",   "type": "string"},
    {"name": "customerId","type": "string"},
    {"name": "amount",    "type": "double"},
    {"name": "currency",  "type": "string"},
    {"name": "createdAt", "type": "long", "logicalType": "timestamp-millis"}
  ]
}
```

### 8.2 Registrar no Schema Registry

```bash
# O subject segue a convenção <topic>-value
curl -X POST http://localhost:8081/subjects/orders-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d "{\"schema\": $(jq -c . order-schema.json | jq -R .)}"

# Resposta: {"id":1}

# Verificar
curl http://localhost:8081/subjects/orders-value/versions/latest | jq .
```

---

## 9. Criar a aplicação producer

### 9.1 Estrutura da aplicação de exemplo

Crie um novo projeto Maven (separado do SDK):

```bash
mkdir orders-producer && cd orders-producer
```

`pom.xml` da aplicação:

```xml
<project>
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.example</groupId>
  <artifactId>orders-producer</artifactId>
  <version>1.0.0</version>

  <repositories>
    <repository>
      <id>confluent</id>
      <url>https://packages.confluent.io/maven/</url>
    </repository>
  </repositories>

  <dependencies>
    <!-- SDK instalado localmente no passo 2 -->
    <dependency>
      <groupId>com.kafka</groupId>
      <artifactId>kafka-producer-sdk</artifactId>
      <version>1.0.0-SNAPSHOT</version>
    </dependency>

    <!-- Para criar GenericRecord Avro -->
    <dependency>
      <groupId>org.apache.avro</groupId>
      <artifactId>avro</artifactId>
      <version>1.11.3</version>
    </dependency>

    <!-- Logging -->
    <dependency>
      <groupId>ch.qos.logback</groupId>
      <artifactId>logback-classic</artifactId>
      <version>1.4.14</version>
    </dependency>
  </dependencies>
</project>
```

### 9.2 Código da aplicação producer

`src/main/java/com/example/OrdersProducer.java`:

```java
package com.example;

import com.kafka.sdk.KafkaProducer;
import com.kafka.sdk.KafkaProducerBuilder;
import com.kafka.sdk.config.ConfigLoader;
import com.kafka.sdk.model.DeliveryResult;
import com.kafka.sdk.model.Message;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrdersProducer {

    private static final Logger log = LoggerFactory.getLogger(OrdersProducer.class);

    private static final String SCHEMA_JSON = """
        {
          "type": "record",
          "name": "Order",
          "namespace": "com.example.orders",
          "fields": [
            {"name": "orderId",    "type": "string"},
            {"name": "customerId", "type": "string"},
            {"name": "amount",     "type": "double"},
            {"name": "currency",   "type": "string"},
            {"name": "createdAt",  "type": "long", "logicalType": "timestamp-millis"}
          ]
        }
        """;

    public static void main(String[] args) throws Exception {
        // Carrega a configuração de variáveis de ambiente — NENHUMA mudança de código
        // entre MSK e Strimzi; apenas as variáveis de ambiente mudam.
        KafkaProducer producer = new KafkaProducerBuilder()
            .withClusterConfig(ConfigLoader.fromEnvironment())
            .build();

        Schema schema = new Schema.Parser().parse(SCHEMA_JSON);

        for (int i = 1; i <= 10; i++) {
            GenericRecord order = new GenericData.Record(schema);
            order.put("orderId",    "order-" + i);
            order.put("customerId", "customer-42");
            order.put("amount",     i * 99.90);
            order.put("currency",   "BRL");
            order.put("createdAt",  System.currentTimeMillis());

            Message msg = Message.forTopic("orders")
                .payload(order)
                .header("source", "orders-producer")
                .header("version", "1.0")
                .build();

            DeliveryResult result = producer.produce(msg).get();

            if (result.isSuccess()) {
                log.info("Entregue: orderId=order-{} partition={} offset={} correlationId={}",
                    i, result.getPartition(), result.getOffset(), result.getCorrelationId());
            } else {
                log.error("Falhou: orderId=order-{} errorCode={} motivo={} correlationId={}",
                    i, result.getErrorCode(), result.getErrorMessage(), result.getCorrelationId());
            }
        }

        producer.close();
        log.info("Producer encerrado.");
    }
}
```

---

## 10. Fase 1 — Produzindo para o MSK

### 10.1 Definir as variáveis de ambiente para MSK

```bash
export KAFKA_SDK_CLUSTER_NAME="msk-production"
export KAFKA_SDK_CLUSTER_TYPE="MSK"
export KAFKA_SDK_BOOTSTRAP_SERVERS="b-1.kafka-migration.abc123.c3.kafka.us-east-1.amazonaws.com:9098,b-2.kafka-migration.abc123.c3.kafka.us-east-1.amazonaws.com:9098"
export KAFKA_SDK_AUTH_MECHANISM="IAM"
export KAFKA_SDK_SCHEMA_REGISTRY_URL="http://localhost:8081"
```

> **Importante:** A máquina local precisa ter permissões IAM para acessar o MSK.
> Se estiver rodando no EKS, associe a IAM Role ao Service Account via IRSA.

### 10.2 Executar o producer

```bash
cd orders-producer
mvn compile exec:java -Dexec.mainClass="com.example.OrdersProducer"
```

Saída esperada:

```
INFO Entregue: orderId=order-1 partition=0 offset=0 correlationId=550e8400-...
INFO Entregue: orderId=order-2 partition=1 offset=0 correlationId=7b3c2100-...
...
INFO Producer encerrado.
```

### 10.3 Verificar mensagens no MSK

```bash
kcat -b <bootstrap-servers-iam> \
  -X security.protocol=SASL_SSL \
  -X sasl.mechanism=AWS_MSK_IAM \
  -X sasl.jaas.config="software.amazon.msk.auth.iam.IAMLoginModule required;" \
  -t orders -C -e -q
```

---

## 11. Fase 2 — Migrando para o Strimzi (mTLS)

O código da aplicação é **exatamente o mesmo**. Somente as variáveis de ambiente mudam.

### 11.1 Acessar o Strimzi de fora do EKS (desenvolvimento local)

Para testes locais, exponha o listener do Strimzi via LoadBalancer ou NodePort:

`strimzi-external-listener.yaml` (adicione ao Kafka CR):

```yaml
# Adicione dentro de spec.kafka.listeners:
      - name: external
        port: 9095
        type: loadbalancer
        tls: true
        authentication:
          type: tls
```

```bash
kubectl apply -f strimzi-kafka.yaml

# Obtenha o endereço externo
kubectl get service kafka-migration-kafka-external-bootstrap -n kafka \
  -o jsonpath='{.status.loadBalancer.ingress[0].hostname}'
# Ex: aaa123.us-east-1.elb.amazonaws.com
```

### 11.2 Definir as variáveis de ambiente para Strimzi mTLS

```bash
# Para teste local via LoadBalancer externo
export STRIMZI_BOOTSTRAP="aaa123.us-east-1.elb.amazonaws.com:9095"
# Para dentro do EKS (pod no mesmo cluster)
# export STRIMZI_BOOTSTRAP="kafka-migration-kafka-bootstrap.kafka.svc.cluster.local:9093"

export KAFKA_SDK_CLUSTER_NAME="strimzi-mtls-production"
export KAFKA_SDK_CLUSTER_TYPE="STRIMZI_MTLS"
export KAFKA_SDK_BOOTSTRAP_SERVERS="$STRIMZI_BOOTSTRAP"
export KAFKA_SDK_AUTH_MECHANISM="MTLS"
export KAFKA_SDK_TLS_KEYSTORE_PATH="/etc/kafka/certs/kafka-keystore.jks"
export KAFKA_SDK_TLS_KEYSTORE_PASS="changeit"
export KAFKA_SDK_TLS_TRUSTSTORE_PATH="/etc/kafka/certs/kafka-truststore.jks"
export KAFKA_SDK_TLS_TRUSTSTORE_PASS="changeit"
export KAFKA_SDK_SCHEMA_REGISTRY_URL="http://localhost:8081"
```

### 11.3 Executar o producer — código idêntico

```bash
# O comando de execução é EXATAMENTE O MESMO — nenhuma mudança
mvn compile exec:java -Dexec.mainClass="com.example.OrdersProducer"
```

Saída esperada (idêntica à fase MSK, cluster name diferente nos logs):

```
INFO Entregue: orderId=order-1 partition=2 offset=0 correlationId=...
INFO Entregue: orderId=order-2 partition=0 offset=0 correlationId=...
...
```

---

## 12. Fase 3 — Migrando para o Strimzi (SCRAM)

Variante alternativa à mTLS — útil quando gestão de certificados é complexa.

### 12.1 Definir as variáveis para SCRAM

```bash
# Recupere a senha do usuário SCRAM criado no passo 6.4
SCRAM_PASSWORD=$(kubectl get secret producer-scram -n kafka \
  -o jsonpath='{.data.password}' | base64 -d)

export KAFKA_SDK_CLUSTER_NAME="strimzi-scram-production"
export KAFKA_SDK_CLUSTER_TYPE="STRIMZI_SCRAM"
export KAFKA_SDK_BOOTSTRAP_SERVERS="<bootstrap-externo-ou-interno>:9094"
export KAFKA_SDK_AUTH_MECHANISM="SCRAM_SHA_512"
export KAFKA_SDK_SCRAM_USERNAME="producer-scram"
export KAFKA_SDK_SCRAM_PASSWORD="$SCRAM_PASSWORD"
export KAFKA_SDK_SCHEMA_REGISTRY_URL="http://localhost:8081"
```

### 12.2 Executar

```bash
# Mesmo comando, zero mudança de código
mvn compile exec:java -Dexec.mainClass="com.example.OrdersProducer"
```

---

## 13. Verificar entrega das mensagens

### 13.1 Consumir do Strimzi via kcat

```bash
# Usando mTLS
kcat -b <bootstrap-mtls> \
  -X security.protocol=SSL \
  -X ssl.key.location=user.key \
  -X ssl.certificate.location=user.crt \
  -X ssl.ca.location=ca.crt \
  -t orders -C -e -q

# Usando SCRAM
kcat -b <bootstrap-scram> \
  -X security.protocol=SASL_SSL \
  -X sasl.mechanism=SCRAM-SHA-512 \
  -X sasl.username=producer-scram \
  -X sasl.password="$SCRAM_PASSWORD" \
  -X ssl.ca.location=ca.crt \
  -t orders -C -e -q
```

### 13.2 Verificar os offsets

```bash
# Número de mensagens no tópico
kcat -b <bootstrap> ... -t orders -C -e -q | wc -l
```

---

## 14. Observabilidade

### 14.1 Métricas com Prometheus + Grafana

Adicione ao producer a dependência `micrometer-registry-prometheus` e exponha uma
instância `PrometheusMeterRegistry`:

```java
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import com.sun.net.httpserver.HttpServer;

PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

// Exponha /metrics via HTTP na porta 8080
HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
server.createContext("/metrics", exchange -> {
    byte[] body = registry.scrape().getBytes();
    exchange.sendResponseHeaders(200, body.length);
    exchange.getResponseBody().write(body);
    exchange.close();
});
server.start();

KafkaProducer producer = new KafkaProducerBuilder()
    .withClusterConfig(ConfigLoader.fromEnvironment())
    .withMeterRegistry(registry)
    .build();
```

Scrape no Prometheus (`prometheus.yml`):

```yaml
scrape_configs:
  - job_name: orders-producer
    static_configs:
      - targets: ["orders-producer-service:8080"]
```

Métricas disponíveis em `http://localhost:8080/metrics`:

```
kafka_sdk_messages_produced_total{topic="orders",cluster="strimzi-mtls-production",outcome="success"} 10.0
kafka_sdk_produce_latency_seconds_max{topic="orders",...} 0.042
```

### 14.2 Tracing com OpenTelemetry + Jaeger

Deploy do Jaeger no EKS:

```bash
kubectl create namespace observability
helm repo add jaegertracing https://jaegertracing.github.io/helm-charts
helm install jaeger jaegertracing/jaeger \
  --namespace observability \
  --set allInOne.enabled=true \
  --set provisionDataStore.cassandra=false \
  --set storage.type=memory
```

Configure o producer para exportar traces:

```bash
# Adicione ao pom.xml:
# io.opentelemetry:opentelemetry-sdk
# io.opentelemetry:opentelemetry-exporter-otlp
```

```java
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;

OtlpGrpcSpanExporter exporter = OtlpGrpcSpanExporter.builder()
    .setEndpoint("http://jaeger-collector.observability.svc.cluster.local:4317")
    .build();

SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
    .addSpanProcessor(BatchSpanProcessor.builder(exporter).build())
    .build();

OpenTelemetry otel = OpenTelemetrySdk.builder()
    .setTracerProvider(tracerProvider)
    .build();

KafkaProducer producer = new KafkaProducerBuilder()
    .withClusterConfig(ConfigLoader.fromEnvironment())
    .withOpenTelemetry(otel)
    .build();
```

Acesse o Jaeger UI:

```bash
kubectl port-forward svc/jaeger-query 16686:16686 -n observability
# Abra: http://localhost:16686
# Service: com.kafka.sdk
```

Você verá spans `kafka.produce` com child spans `kafka.schema.validate` e `kafka.client.send`,
todos com o atributo `kafka.sdk.correlation_id` compartilhado com os logs e métricas.

---

## 15. Limpeza de recursos

```bash
# Strimzi / EKS
kubectl delete kafka kafka-migration -n kafka
kubectl delete namespace kafka schema-registry observability
eksctl delete cluster --name kafka-migration-eks --region us-east-1

# MSK
aws kafka delete-cluster --cluster-arn <arn-do-cluster>

# IAM
aws iam delete-policy --policy-arn arn:aws:iam::<account>:policy/MskProducerPolicy
```

---

## 16. Solução de problemas comuns

### `ConfigurationException: Schema Registry unreachable`

```bash
# Verifique se o port-forward está ativo
curl http://localhost:8081/subjects

# Se retornar "Connection refused", reative o port-forward
kubectl port-forward svc/schema-registry-cp-schema-registry 8081:8081 -n schema-registry &
```

### `SCHEMA_VALIDATION_FAILED: No schema registered for subject 'orders-value'`

```bash
# Verifique se o schema foi registrado
curl http://localhost:8081/subjects

# Re-registre se necessário (passo 8.2)
```

### `AUTH_FAILED` no Strimzi mTLS

```bash
# Verifique se o certificado do usuário é válido
openssl verify -CAfile ca.crt user.crt
# Deve retornar: user.crt: OK

# Verifique a data de expiração
openssl x509 -in user.crt -noout -dates
```

### `AUTH_FAILED` no Strimzi SCRAM

```bash
# Confirme a senha atual do KafkaUser
kubectl get secret producer-scram -n kafka \
  -o jsonpath='{.data.password}' | base64 -d
echo   # nova linha

# Teste a conectividade com kcat antes de usar o SDK
kcat -b <bootstrap>:9094 \
  -X security.protocol=SASL_SSL \
  -X sasl.mechanism=SCRAM-SHA-512 \
  -X sasl.username=producer-scram \
  -X sasl.password="<senha>" \
  -X ssl.ca.location=ca.crt \
  -L
```

### `DELIVERY_TIMEOUT` — mensagens não chegam

```bash
# Verifique se o tópico existe no cluster de destino
kubectl get kafkatopic orders -n kafka

# Verifique conectividade de rede do pod producer ao Kafka
kubectl exec -it <pod-producer> -n <namespace> -- \
  nc -zv kafka-migration-kafka-bootstrap.kafka.svc.cluster.local 9093
```

### Certificados expirados no Strimzi

O Strimzi renova certificados automaticamente a cada 30 dias. Se o certificado do usuário
expirar, force a renovação:

```bash
# Anote e remova a annotation de expiração para forçar renovação
kubectl annotate kafkauser producer-mtls \
  strimzi.io/force-renew=true -n kafka
```

---

## Resumo da Migração

```
┌──────────────────────────────────────────────────────────────────┐
│                    CÓDIGO DA APLICAÇÃO                           │
│                   (NUNCA MUDA)                                   │
│                                                                  │
│  KafkaProducer producer = new KafkaProducerBuilder()            │
│      .withClusterConfig(ConfigLoader.fromEnvironment())         │
│      .build();                                                   │
│                                                                  │
│  producer.produce(Message.forTopic("orders")                    │
│      .payload(orderEvent).build()).get();                        │
└──────────────────────────────────────────────────────────────────┘
              │                           │
              ▼                           ▼
  ┌─────────────────────┐   ┌──────────────────────────┐
  │   MSK (IAM)         │   │   Strimzi (mTLS/SCRAM)   │
  │                     │   │                          │
  │ KAFKA_SDK_CLUSTER_  │   │ KAFKA_SDK_CLUSTER_       │
  │   TYPE=MSK          │   │   TYPE=STRIMZI_MTLS      │
  │ KAFKA_SDK_AUTH_     │   │ KAFKA_SDK_AUTH_          │
  │   MECHANISM=IAM     │   │   MECHANISM=MTLS         │
  │ KAFKA_SDK_BOOTSTRAP │   │ KAFKA_SDK_BOOTSTRAP      │
  │   _SERVERS=b-1....  │   │   _SERVERS=kafka-boot... │
  └─────────────────────┘   └──────────────────────────┘
```

A migração entre os dois clusters é um **kubectl rollout restart** com um ConfigMap
ou Secret atualizado — sem redeploy de código.
