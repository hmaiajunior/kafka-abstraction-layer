# Multi-stage build: installs the SDK into the local Maven repo, then builds the demo
# shaded jar that bundles both DemoProducer (uses the SDK) and DemoConsumer (vanilla).
FROM maven:3.9-eclipse-temurin-17 AS build

WORKDIR /workspace

# Build & install the SDK first so demo/ can resolve it as a normal dependency.
COPY pom.xml ./
COPY checkstyle.xml ./
COPY src ./src
RUN mvn -q -DskipTests -Dcheckstyle.skip=true -Dspotbugs.skip=true install

# Now build the demo jar (depends on the SDK installed above).
COPY demo/pom.xml demo/pom.xml
COPY demo/src demo/src
WORKDIR /workspace/demo
RUN mvn -q -DskipTests package

# Runtime image: bundles the shaded jar; the actual main class is selected via
# `command:` in docker-compose so producer/consumer share one image.
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /workspace/demo/target/kafka-migration-demo.jar /app/demo.jar
ENTRYPOINT ["java"]
CMD ["-cp", "/app/demo.jar", "com.kafka.demo.DemoProducer"]
