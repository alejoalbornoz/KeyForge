# syntax=docker/dockerfile:1
# The KeyForge server, built from source: docker-compose.yml (the quick start) runs this image three times.
# Building needs nothing on the host but Docker; Maven's downloads are cached between builds.

FROM eclipse-temurin:25-jdk AS build
WORKDIR /src
COPY . .
# mvnw is not executable in a Windows checkout, hence "sh mvnw".
RUN --mount=type=cache,target=/root/.m2 \
    sh mvnw -B -q -pl keyforge-server -am package -DskipTests \
    && cp keyforge-server/target/keyforge-server-*-all.jar /keyforge-server.jar

FROM eclipse-temurin:25-jre
COPY --from=build /keyforge-server.jar /app/keyforge-server.jar
WORKDIR /data
# RESP clients, the Raft transport, Prometheus (when metrics.port is set).
EXPOSE 6379 7000 9464
ENTRYPOINT ["java", "-jar", "/app/keyforge-server.jar"]
