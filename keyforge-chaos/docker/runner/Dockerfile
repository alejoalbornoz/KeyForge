# The linearizability test's runner (keyforge-chaos/scripts/linearizability.sh starts it with "docker compose run").
# It runs on the cluster's network, so clients reach the nodes directly: a killed server's port refuses connections,
# and a client knows its operation was never sent (ADR 0016). The Docker command line, talking to the host's Docker
# through the mounted socket, lets the nemesis drive the containers. The runner jar is mounted from the build.
FROM docker:29.0-cli AS cli

FROM eclipse-temurin:25-jdk
COPY --from=cli /usr/local/bin/docker /usr/local/bin/docker
ENTRYPOINT ["java", "-jar", "/app/chaos.jar"]
