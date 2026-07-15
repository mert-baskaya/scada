FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline -B -q
COPY src ./src
RUN mvn package -DskipTests -B -q

FROM eclipse-temurin:21-jre
COPY --from=build /app/target/spring-api-1.0-SNAPSHOT.jar /app/spring-api.jar
ENTRYPOINT ["java", "-jar", "/app/spring-api.jar"]
