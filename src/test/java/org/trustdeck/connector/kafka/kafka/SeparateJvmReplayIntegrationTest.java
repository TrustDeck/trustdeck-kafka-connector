/*
 * TrustDeck Kafka Connector
 * Copyright 2026 Armin Müller
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.trustdeck.connector.kafka.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.trustdeck.connector.kafka.recovery.RecoveryEnvelope;

/** Verifies that a replay envelope can be executed by a separate connector JVM. */
@Testcontainers
class SeparateJvmReplayIntegrationTest {

    private static final String SOURCE = "separate-replay-" + UUID.randomUUID();
    private static final String DLT = SOURCE + ".DLT";
    private static final String GROUP = SOURCE + "-group";
    private static final String UUID_VALUE = "11111111-1111-1111-1111-111111111111";

    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.0");

    private HttpServer trustDeck;
    private final List<String> paths = new CopyOnWriteArrayList<>();

    /** Creates isolated topics and a controlled TrustDeck endpoint. */
    @BeforeEach
    void start() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(SOURCE, 1, (short) 1), new NewTopic(DLT, 1, (short) 1)))
                    .all().get(30, TimeUnit.SECONDS);
        }
        trustDeck = HttpServer.create(new InetSocketAddress(0), 0);
        trustDeck.createContext("/", this::respond);
        trustDeck.start();
    }

    /** Stops the controlled endpoint. */
    @AfterEach
    void stop() {
        if (trustDeck != null) {
            trustDeck.stop(0);
        }
    }

    /** Posts a preserved combined-operation envelope through a separately started JVM. */
    @Test
    void executesCombinedReplayInSeparateJvm() throws Exception {
        byte[] value = "{}".getBytes(StandardCharsets.UTF_8);
        RecoveryEnvelope envelope = new RecoveryEnvelope("2", "FAILED_DLT", SOURCE, 0, 7,
                null, "CREATE_ENTITY_AND_PSEUDONYM", "synthetic-domain", "TRANSIENT_OPERATION_FAILURE",
                503, 3, "ENTITY_CREATED", UUID_VALUE, null,
                Base64.getEncoder().encodeToString(value), null, Map.of(), "replay", "synthetic-project",
                "synthetic-type");
        String json = new ObjectMapper().writeValueAsString(envelope);
        int replayPort = freePort();
        Path childLog = Files.createTempFile("separate-replay-", ".log");
        Process process = new ProcessBuilder(javaCommand(), "-cp", testClasspath(),
                SeparateJvmReplayProcess.class.getName(), Integer.toString(replayPort), json, trustDeckUrl(),
                KAFKA.getBootstrapServers(), SOURCE, GROUP, DLT).redirectErrorStream(true)
                .redirectOutput(childLog.toFile()).start();
        try {
            assertTrue(process.waitFor(60, TimeUnit.SECONDS), Files.readString(childLog));
            assertEquals(0, process.exitValue(), Files.readString(childLog));
            assertTrue(paths.stream().anyMatch(path -> path.contains("/pseudonyms")));
            assertEquals(0, paths.stream().filter(path -> path.equals("POST /api/projects/synthetic-project/entities/synthetic-type"))
                    .count());
        } finally {
            process.destroyForcibly();
            Files.deleteIfExists(childLog);
        }
    }

    /** Serves OAuth and records the operation stage used by the replay. */
    private void respond(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        paths.add(exchange.getRequestMethod() + " " + path);
        if (path.equals("/token")) {
            write(exchange, 200, "{\"access_token\":\"replay-token\",\"expires_in\":300}");
        } else if (path.contains("/pseudonyms") && "GET".equals(exchange.getRequestMethod())) {
            write(exchange, 200, "[]");
        } else if (path.contains("/pseudonyms")) {
            write(exchange, 201,
                    "{\"identifierItem\":{\"identifier\":\"replayed\",\"idType\":\"source\"},"
                            + "\"psn\":\"replayed\",\"trustdeckID\":\"" + UUID_VALUE + "\",\"data\":{}}");
        } else {
            write(exchange, 200, "{}");
        }
    }

    private String trustDeckUrl() {
        return "http://localhost:" + trustDeck.getAddress().getPort();
    }

    private int freePort() throws IOException {
        try (java.net.ServerSocket socket = new java.net.ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private String javaCommand() {
        return System.getProperty("java.home") + "/bin/java";
    }

    private String testClasspath() {
        return System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    }

    private void write(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    /** Starts the real application and invokes the guarded endpoint from another JVM. */
    public static final class SeparateJvmReplayProcess {
        private SeparateJvmReplayProcess() {
        }

        /** Runs one replay and exits with the endpoint result. */
        public static void main(String[] args) throws Exception {
            int port = Integer.parseInt(args[0]);
            String envelope = args[1];
            String serviceUrl = args[2];
            String bootstrapServers = args[3];
            String source = args[4];
            String group = args[5];
            String dlt = args[6];
            org.springframework.context.ConfigurableApplicationContext context =
                    new org.springframework.boot.builder.SpringApplicationBuilder(
                            org.trustdeck.connector.kafka.TrustDeckKafkaConnectorApplication.class).run(
                                    "--server.port=" + port, "--spring.kafka.bootstrap-servers=" + bootstrapServers,
                                    "--spring.kafka.properties.security.protocol=PLAINTEXT",
                                    "--spring.kafka.consumer.properties.security.protocol=PLAINTEXT",
                                    "--spring.kafka.producer.properties.security.protocol=PLAINTEXT",
                                    "--spring.kafka.listener.missing-topics-fatal=false",
                                    "--connector.operation=CREATE_ENTITY_AND_PSEUDONYM", "--connector.source-topic=" + source,
                                    "--connector.group-id=" + group, "--connector.recovery.dlt-topic=" + dlt,
                                    "--connector.recovery.review-topic=" + source + ".REVIEW", "--connector.preflight.enabled=false",
                                    "--connector.replay.enabled=true", "--connector.trustdeck.service-url=" + serviceUrl,
                                    "--connector.trustdeck.token-endpoint=" + serviceUrl + "/token",
                                    "--connector.trustdeck.client-id=replay-client", "--connector.trustdeck.client-secret=replay-secret",
                                    "--connector.trustdeck.project=synthetic-project", "--connector.trustdeck.entity-type=synthetic-type",
                                    "--connector.mapping.common.domain-constant=synthetic-domain");
            try {
                HttpClient client = HttpClient.newHttpClient();
                HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/internal/replay"))
                        .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(envelope)).build();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
                HttpResponse<String> response;
                do {
                    try {
                        response = client.send(request, HttpResponse.BodyHandlers.ofString());
                        if (response.statusCode() == 200) {
                            System.exit(0);
                        }
                    } catch (java.net.ConnectException ignored) {
                        // The embedded web server may still be starting.
                    }
                    Thread.sleep(100);
                } while (System.nanoTime() < deadline);
                throw new AssertionError("Replay endpoint did not complete");
            } finally {
                context.close();
            }
        }
    }
}
