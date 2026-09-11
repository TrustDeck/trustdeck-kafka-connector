/*
 * TrustDeck Kafka Connector
 * Copyright 2026 Armin Müller
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.trustdeck.connector.kafka.trustdeck;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.trustdeck.connector.kafka.config.ConnectorProperties;

/** Verifies OAuth refresh before the configured token expiry window. */
class ClientCredentialsTokenProviderTest {

    private HttpServer server;
    private AtomicInteger requests;

    @BeforeEach
    void start() throws IOException {
        requests = new AtomicInteger();
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/token", this::token);
        server.start();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    /** An almost-expired token is refreshed rather than reused. */
    @Test
    void refreshesExpiringToken() {
        ConnectorProperties properties = new ConnectorProperties();
        properties.getTrustdeck().setTokenEndpoint("http://localhost:" + server.getAddress().getPort() + "/token");
        properties.getTrustdeck().setClientId("client");
        properties.getTrustdeck().setClientSecret("secret");
        ClientCredentialsTokenProvider provider = new ClientCredentialsTokenProvider(properties);

        assertEquals("token-1", provider.getAccessToken());
        assertEquals("token-2", provider.getAccessToken());
        assertEquals(2, requests.get());
    }

    private void token(HttpExchange exchange) throws IOException {
        int number = requests.incrementAndGet();
        byte[] response = ("{\"access_token\":\"token-" + number + "\",\"expires_in\":1}")
                .getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, response.length);
        exchange.getResponseBody().write(response);
        exchange.close();
    }
}
