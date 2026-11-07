FROM maven:3.9.11-eclipse-temurin-25 AS build
WORKDIR /build
COPY pom.xml .
COPY src src
RUN mvn -B -DskipTests package
FROM eclipse-temurin:25-jre
RUN groupadd --gid 10001 broker && useradd --uid 10001 --gid broker --create-home broker && mkdir -p /app/data && chown -R broker:broker /app
WORKDIR /app
COPY --from=build --chown=broker:broker /build/target/mini-rabbitmq-1.0.0.jar /app/minirabbit.jar
COPY --chown=broker:broker configs/docker.yaml /app/configs/broker.yaml
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["java","-XX:MaxRAMPercentage=65","-jar","/app/minirabbit.jar"]
CMD ["server","start"]
