# syntax=docker/dockerfile:1.7
FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY pom.xml .
RUN --mount=type=cache,target=/root/.m2 mvn -B -q dependency:go-offline
COPY src ./src
RUN --mount=type=cache,target=/root/.m2 sh -c 'for attempt in 1 2 3; do mvn -B -DskipTests package && exit 0; echo "Maven build failed (attempt ${attempt}/3); retrying in 5 seconds..." >&2; sleep 5; done; exit 1'

FROM eclipse-temurin:21.0.7_6-jre
WORKDIR /app
RUN useradd --system --uid 10001 --create-home appuser
COPY --from=build /workspace/target/kafka-performance-lab-1.0.0.jar /app/app.jar
USER 10001
EXPOSE 8080
ENTRYPOINT ["java","-XX:MaxRAMPercentage=75.0","-jar","/app/app.jar"]
