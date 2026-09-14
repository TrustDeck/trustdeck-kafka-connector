/*
 * TrustDeck Kafka Connector
 * Copyright 2026 Armin Müller
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.trustdeck.connector.kafka.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.trustdeck.connector.kafka.TrustDeckKafkaConnectorApplication;

/** Exercises the configured Spring Kafka listener and recovery publisher against disposable services. */
@SpringBootTest(classes = TrustDeckKafkaConnectorApplication.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Testcontainers
class ConnectorKafkaIntegrationTest {

    private static final String SOURCE = "connector-source-" + UUID.randomUUID();
    private static final String DLT = SOURCE + ".DLT";
    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.8.0"));
    private static HttpServer trustDeck;

    static {
        try {
            trustDeck = HttpServer.create(new InetSocketAddress(0), 0);
            trustDeck.createContext("/", ConnectorKafkaIntegrationTest::respond);
            trustDeck.start();
        } catch (IOException exception) {
            throw new ExceptionInInitializerError(exception);
        }
    }

    /** Supplies isolated Kafka and HTTP-stub settings before the real application context starts. */
    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(java.util.List.of(new NewTopic(SOURCE, 2, (short) 1),
                    new NewTopic(DLT, 1, (short) 1), new NewTopic(SOURCE + ".REVIEW", 1, (short) 1)))
                    .all().get(15, TimeUnit.SECONDS);
        } catch (Exception exception) {
            throw new IllegalStateException("Could not create integration topics", exception);
        }
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.kafka.properties.security.protocol", () -> "PLAINTEXT");
        registry.add("spring.kafka.consumer.properties.security.protocol", () -> "PLAINTEXT");
        registry.add("spring.kafka.producer.properties.security.protocol", () -> "PLAINTEXT");
        registry.add("connector.operation", () -> "CREATE_ENTITY");
        registry.add("connector.source-topic", () -> SOURCE);
        registry.add("connector.group-id", () -> "connector-integration-group");
        registry.add("connector.recovery.dlt-topic", () -> DLT);
        registry.add("connector.recovery.review-topic", () -> SOURCE + ".REVIEW");
        registry.add("connector.trustdeck.service-url", () -> "http://localhost:" + trustDeck.getAddress().getPort());
        registry.add("connector.trustdeck.token-endpoint", () -> "http://localhost:" + trustDeck.getAddress().getPort() + "/token");
        registry.add("connector.trustdeck.client-id", () -> "integration-client");
        registry.add("connector.trustdeck.client-secret", () -> "integration-secret");
        registry.add("connector.trustdeck.project", () -> "synthetic-project");
        registry.add("connector.trustdeck.entity-type", () -> "synthetic-type");
        registry.add("spring.kafka.listener.missing-topics-fatal", () -> "false");
    }

    /** Confirms the application context is ready before records are produced. */
    @BeforeAll
    static void ready() {
        assertNotNull(KAFKA.getBootstrapServers());
    }

    /** Sends a valid record through the real listener and observes the TrustDeck HTTP effect. */
    @Test
    void realListenerProcessesRecordAndCommitsIt() throws Exception {
        byte[] value = "{}".getBytes(StandardCharsets.UTF_8);
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(byteProducerSettings())) {
            var metadata = producer.send(new ProducerRecord<>(SOURCE, new byte[] {1}, value))
                    .get(15, TimeUnit.SECONDS);
            awaitCommitted(metadata.topic(), metadata.partition(), metadata.offset() + 1);
        }
        awaitRequest("POST", "/api/projects/synthetic-project/entities/synthetic-type");
        assertEquals(1, requestCount);
    }

    /** Sends malformed input through the real listener and observes the confirmed DLT envelope and commit. */
    @Test
    void realListenerPublishesDltBeforeCommittingInvalidRecord() throws Exception {
        byte[] invalid = "{invalid".getBytes(StandardCharsets.UTF_8);
        var metadata = send(new ProducerRecord<>(SOURCE, new byte[] {2}, invalid));
        String envelope = pollRecovery();
        assertNotNull(envelope);
        assertEquals(true, envelope.contains("\"status\":\"FAILED_DLT\""));
        awaitCommitted(metadata.topic(), metadata.partition(), metadata.offset() + 1);
    }

    /** Stops the controlled server after all application resources are closed. */
    @AfterAll
    static void stopStub() {
        trustDeck.stop(0);
    }

    private static int requestCount;
    private static final List<String> requestPaths = new java.util.concurrent.CopyOnWriteArrayList<>();

    private static Map<String, Object> byteProducerSettings() {
        return Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    }

    private static org.apache.kafka.clients.producer.RecordMetadata send(ProducerRecord<byte[], byte[]> record)
            throws Exception {
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(byteProducerSettings())) {
            return producer.send(record).get(15, TimeUnit.SECONDS);
        }
    }

    private static String pollRecovery() {
        Map<String, Object> settings = Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "recovery-observer-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(settings)) {
            consumer.subscribe(List.of(DLT));
            while (System.nanoTime() < deadline) {
                var records = consumer.poll(java.time.Duration.ofMillis(250));
                if (!records.isEmpty()) {
                    return records.iterator().next().value();
                }
            }
        }
        throw new AssertionError("No recovery envelope was observed");
    }

    private static void awaitRequest(String method, String path) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while ((!requestPaths.contains(path) || requestCount == 0) && System.nanoTime() < deadline) {
            Thread.sleep(100);
        }
        assertEquals(1, requestCount);
        assertEquals(true, requestPaths.contains(path));
    }

    private static void awaitCommitted(String topic, int partition, long expectedOffset) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        TopicPartition source = new TopicPartition(topic, partition);
        while (System.nanoTime() < deadline) {
            try (var admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
                var committed = admin.listConsumerGroupOffsets("connector-integration-group")
                        .partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS).get(source);
                if (committed != null && committed.offset() >= expectedOffset) {
                    return;
                }
            }
            Thread.sleep(100);
        }
        throw new AssertionError("Expected committed offset " + expectedOffset + " for " + source);
    }

    private static void respond(HttpExchange exchange) throws IOException {
        requestPaths.add(exchange.getRequestURI().getPath());
        if (exchange.getRequestURI().getPath().equals("/token")) {
            byte[] response = "{\"access_token\":\"integration-token\",\"expires_in\":300}"
                    .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
        } else if ("GET".equals(exchange.getRequestMethod())) {
            byte[] response = "{}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
        } else if ("POST".equals(exchange.getRequestMethod())) {
            requestCount++;
            byte[] response = "{\"trustdeckID\":\"11111111-1111-1111-1111-111111111111\",\"data\":{}}"
                    .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(201, response.length);
            exchange.getResponseBody().write(response);
        } else {
            exchange.sendResponseHeaders(404, -1);
        }
        exchange.close();
    }
}
