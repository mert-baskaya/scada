FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline -B -q
COPY src ./src
RUN mvn package -DskipTests -B -q

FROM eclipse-temurin:21-jre
COPY --from=build /app/target/scada-load-generator-1.0-SNAPSHOT.jar /app/load-generator.jar
ENTRYPOINT ["java", "-Xms1g", "-Xmx1536m", "-jar", "/app/load-generator.jar"]
