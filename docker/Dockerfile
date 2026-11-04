# syntax=docker/dockerfile:1.7
FROM maven:3.9.11-eclipse-temurin-21 AS build
WORKDIR /build
# Uses HTTP(S)_PROXY/NO_PROXY build arguments for Maven; a maven_settings secret takes precedence
COPY --chmod=755 docker/maven-proxy.sh /usr/local/bin/mvn-proxy
COPY pom.xml .
RUN --mount=type=secret,id=maven_settings,target=/root/.m2/settings.xml,required=false mvn-proxy -B dependency:go-offline
COPY src src
RUN --mount=type=secret,id=maven_settings,target=/root/.m2/settings.xml,required=false mvn-proxy -B clean package -DskipTests

FROM eclipse-temurin:21-jre-jammy
RUN apt-get update \
    && apt-get install --no-install-recommends -y curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --create-home connector
ENV TZ=UTC JAVA_TOOL_OPTIONS="-Duser.timezone=UTC"
WORKDIR /app
COPY --from=build /build/target/trustdeck-kafka-connector-1.0.0.jar /app/app.jar
RUN chown -R connector:connector /app
USER connector
EXPOSE 8080
ENTRYPOINT ["java", "-XX:+ExitOnOutOfMemoryError", "-jar", "/app/app.jar"]
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 CMD curl --fail --silent http://127.0.0.1:8080/actuator/health/readiness || exit 1
