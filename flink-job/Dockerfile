FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline -B -q
COPY src ./src
RUN mvn package -DskipTests -B -q

FROM flink:2.0.0-java21
# Pre-create so the named volume inherits flink-user ownership on first mount
RUN mkdir -p /opt/flink/checkpoints && chown flink:flink /opt/flink/checkpoints
COPY --from=build /app/target/flink-scada-job-1.0-SNAPSHOT.jar /opt/flink/usrlib/flink-scada-job.jar
