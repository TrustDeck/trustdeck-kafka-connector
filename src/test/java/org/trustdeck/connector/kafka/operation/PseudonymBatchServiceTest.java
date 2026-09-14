/*
 * TrustDeck Kafka Connector
 * Copyright 2026 Armin Müller
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.trustdeck.connector.kafka.operation;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.trustdeck.client.TrustDeckClient;
import org.trustdeck.connector.kafka.config.ConnectorProperties;
import org.trustdeck.connector.kafka.mapping.MappedMessage;

/** Verifies safe reconciliation of a known partial batch response. */
class PseudonymBatchServiceTest {

    private HttpServer server;
    private int singleCreates;
    private String batchBody;

    @BeforeEach
    void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/", this::respond);
        server.start();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    /** Reconciles one returned success and creates only the unresolved occurrence singly. */
    @Test
    void fallsBackToSinglesForKnownPartialResponse() {
        ConnectorProperties properties = new ConnectorProperties();
        properties.setBatchSize(10);
        properties.setBatchBytes(100_000);
        TrustDeckClient client = new TrustDeckClient("http://localhost:" + server.getAddress().getPort(), () -> "token");
        ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
        List<MappedMessage> messages = List.of(message("one", "p1"), message("two", "p2"));

        List<org.trustdeck.client.model.Pseudonym> result = new PseudonymBatchService(client, properties, mapper)
                .execute(messages);

        assertEquals(2, result.size());
        assertEquals(1, singleCreates);
    }

    /** Verifies an optional validity duration is retained in a batch request. */
    @Test
    void forwardsOptionalValidityTimeInBatch() {
        ConnectorProperties properties = new ConnectorProperties();
        properties.setBatchSize(10);
        properties.setBatchBytes(100_000);
        TrustDeckClient client = new TrustDeckClient("http://localhost:" + server.getAddress().getPort(), () -> "token");
        ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

        new PseudonymBatchService(client, properties, mapper).execute(List.of(message("one", "p1", "1d")));

        assertEquals(true, batchBody.contains("\"validityTime\":\"1d\""));
    }

    private MappedMessage message(String identifier, String psn) {
        return new MappedMessage("domain", identifier, "type", psn, null, null,
                null, null, null, null, null, null, null, null, null, null, null, null);
    }

    private MappedMessage message(String identifier, String psn, String validityTime) {
        return new MappedMessage("domain", identifier, "type", psn, validityTime, null, null,
                null, null, null, null, null, null, null, null, null, null, null, null);
    }

    private void respond(HttpExchange exchange) throws IOException {
        boolean batch = exchange.getRequestURI().getPath().endsWith("/batch");
        batchBody = new String(exchange.getRequestBody().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        if (!batch) {
            singleCreates++;
        }
        String response = "[{\"identifierItem\":{\"identifier\":\"one\",\"idType\":\"type\"},\"psn\":\"p1\"}]";
        if (!batch) {
            response = "{\"identifierItem\":{\"identifier\":\"two\",\"idType\":\"type\"},\"psn\":\"p2\"}";
        }
        byte[] body = response.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(batch ? 206 : 201, body.length);
        exchange.getResponseBody().write(body);
        exchange.close();
    }
}
