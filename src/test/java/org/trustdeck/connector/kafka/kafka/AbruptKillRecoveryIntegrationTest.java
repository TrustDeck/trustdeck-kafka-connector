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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.trustdeck.connector.kafka.TrustDeckKafkaConnectorApplication;

/** Verifies redelivery after the connector JVM is killed before an HTTP response. */
@Testcontainers
class AbruptKillRecoveryIntegrationTest {

    private static final String SOURCE = "abrupt-kill-" + UUID.randomUUID();
    private static final String GROUP = SOURCE + "-group";

    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.8.0"));

    private HttpServer trustDeck;
    private final List<String> markers = new CopyOnWriteArrayList<>();
    private final CountDownLatch requestStarted = new CountDownLatch(1);
    private final AtomicBoolean blockRequest = new AtomicBoolean(true);
    private ConfigurableApplicationContext context;

    /** Creates one isolated source topic and a blocking TrustDeck endpoint. */
    @BeforeEach
    void start() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(SOURCE, 1, (short) 1))).all().get(30, TimeUnit.SECONDS);
        }
        trustDeck = HttpServer.create(new InetSocketAddress(0), 0);
        trustDeck.createContext("/", this::respond);
        trustDeck.start();
    }

    /** Closes the connector and controlled endpoint. */
    @AfterEach
    void stop() {
        if (context != null) {
            context.close();
        }
        if (trustDeck != null) {
            trustDeck.stop(0);
        }
    }

    /** Kills the first process during its HTTP call and verifies the source record is redelivered. */
    @Test
    void redeliversAfterAbruptJvmKill() throws Exception {
        Path childLog = Files.createTempFile("abrupt-kill-", ".log");
        Process process = new ProcessBuilder(System.getProperty("java.home") + "/bin/java", "-cp",
                System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
                AbruptKillProcess.class.getName(), KAFKA.getBootstrapServers(), SOURCE, GROUP, serviceUrl())
                .redirectErrorStream(true).redirectOutput(childLog.toFile()).start();
        try {
            send("abrupt");
            assertTrue(requestStarted.await(60, TimeUnit.SECONDS), Files.readString(childLog));
            process.destroyForcibly();
            process.waitFor(10, TimeUnit.SECONDS);
            blockRequest.set(false);

            context = startConnector();
            awaitMarkerCount(2);
            awaitCommitted(1);
            assertEquals(List.of("abrupt", "abrupt"), markers);
        } finally {
            process.destroyForcibly();
            Files.deleteIfExists(childLog);
        }
    }

    private ConfigurableApplicationContext startConnector() {
        return new SpringApplicationBuilder(TrustDeckKafkaConnectorApplication.class).web(WebApplicationType.NONE)
                .run("--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                        "--spring.kafka.properties.security.protocol=PLAINTEXT",
                        "--spring.kafka.consumer.properties.security.protocol=PLAINTEXT",
                        "--spring.kafka.producer.properties.security.protocol=PLAINTEXT",
                        "--spring.kafka.consumer.auto-offset-reset=earliest", "--spring.kafka.consumer.max-poll-records=1",
                        "--spring.kafka.consumer.properties.session.timeout.ms=10000",
                        "--spring.kafka.consumer.properties.heartbeat.interval.ms=3000",
                        "--connector.operation=CREATE_ENTITY", "--connector.source-topic=" + SOURCE,
                        "--connector.group-id=" + GROUP, "--connector.recovery.dlt-topic=" + SOURCE + ".DLT",
                        "--connector.recovery.review-topic=" + SOURCE + ".REVIEW", "--connector.preflight.enabled=false",
                        "--connector.trustdeck.service-url=" + serviceUrl(),
                        "--connector.trustdeck.token-endpoint=" + serviceUrl() + "/token",
                        "--connector.trustdeck.client-id=kill-client", "--connector.trustdeck.client-secret=kill-secret",
                        "--connector.trustdeck.project=synthetic-project", "--connector.trustdeck.entity-type=synthetic-type",
                        "--connector.mapping.common.entity-fields[0].source-path=/marker",
                        "--connector.mapping.common.entity-fields[0].target-path=marker");
    }

    private void send(String marker) throws Exception {
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class))) {
            producer.send(new ProducerRecord<>(SOURCE, 0, null,
                    ("{\"marker\":\"" + marker + "\"}").getBytes(StandardCharsets.UTF_8))).get(15, TimeUnit.SECONDS);
        }
    }

    private void respond(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        if (path.equals("/token")) {
            write(exchange, 200, "{\"access_token\":\"kill-token\",\"expires_in\":300}");
            return;
        }
        String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        String marker = body.replaceAll(".*\\\"marker\\\":\\\"([^\\\"]+)\\\".*", "$1");
        markers.add(marker);
        requestStarted.countDown();
        while (blockRequest.get()) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        write(exchange, 201, "{\"trustdeckID\":\"11111111-1111-1111-1111-111111111111\",\"data\":{}}");
    }

    private void awaitMarkerCount(int expected) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (markers.size() < expected && System.nanoTime() < deadline) {
            Thread.sleep(50);
        }
        assertEquals(expected, markers.size());
    }

    private void awaitCommitted(long expected) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (System.nanoTime() < deadline) {
            try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
                var offset = admin.listConsumerGroupOffsets(GROUP).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS)
                        .get(new TopicPartition(SOURCE, 0));
                if (offset != null && offset.offset() >= expected) {
                    return;
                }
            }
            Thread.sleep(100);
        }
        throw new AssertionError("Expected committed source offset " + expected);
    }

    private String serviceUrl() {
        return "http://localhost:" + trustDeck.getAddress().getPort();
    }

    private void write(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    /** Runs a connector in a child JVM until its process is terminated by the test. */
    public static final class AbruptKillProcess {
        private AbruptKillProcess() {
        }

        /** Starts the real application with the same group as the parent test. */
        public static void main(String[] args) {
            new SpringApplicationBuilder(TrustDeckKafkaConnectorApplication.class).web(WebApplicationType.NONE).run(
                    "--spring.kafka.bootstrap-servers=" + args[0], "--spring.kafka.properties.security.protocol=PLAINTEXT",
                    "--spring.kafka.consumer.properties.security.protocol=PLAINTEXT",
                    "--spring.kafka.producer.properties.security.protocol=PLAINTEXT",
                    "--spring.kafka.listener.missing-topics-fatal=false",
                    "--spring.kafka.consumer.properties.session.timeout.ms=10000",
                    "--spring.kafka.consumer.properties.heartbeat.interval.ms=3000",
                    "--connector.operation=CREATE_ENTITY", "--connector.source-topic=" + args[1],
                    "--connector.group-id=" + args[2], "--connector.recovery.dlt-topic=" + args[1] + ".DLT",
                    "--connector.recovery.review-topic=" + args[1] + ".REVIEW", "--connector.preflight.enabled=false",
                    "--connector.trustdeck.service-url=" + args[3], "--connector.trustdeck.token-endpoint=" + args[3] + "/token",
                    "--connector.trustdeck.client-id=kill-client", "--connector.trustdeck.client-secret=kill-secret",
                    "--connector.trustdeck.project=synthetic-project", "--connector.trustdeck.entity-type=synthetic-type",
                    "--connector.mapping.common.entity-fields[0].source-path=/marker",
                    "--connector.mapping.common.entity-fields[0].target-path=marker");
        }
    }
}
