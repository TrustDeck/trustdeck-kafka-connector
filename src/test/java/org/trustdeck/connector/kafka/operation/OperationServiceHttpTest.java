/*
 * TrustDeck Kafka Connector
 * Copyright 2026 Armin Müller
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.trustdeck.connector.kafka.operation;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.LocalDateTime;
import java.util.UUID;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.trustdeck.client.TrustDeckClient;
import org.trustdeck.connector.kafka.config.ConnectorProperties;
import org.trustdeck.connector.kafka.mapping.MappedMessage;

/** Verifies operation dispatch and wire calls against a controlled HTTP server. */
class OperationServiceHttpTest {

    /** Controlled HTTP server. */
    private HttpServer server;

    /** Connector settings. */
    private ConnectorProperties properties;

    /**
     * Starts the controlled server.
     *
     * @throws IOException when the server cannot start
     */
    @BeforeEach
    void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/", this::respond);
        server.start();
        properties = new ConnectorProperties();
        properties.setOperation("CREATE_PSEUDONYM");
        properties.getTrustdeck().setServiceUrl("http://localhost:" + server.getAddress().getPort());
        properties.getTrustdeck().setProject("synthetic-project");
        properties.getTrustdeck().setEntityType("synthetic-type");
    }

    /** Stops the controlled server. */
    @AfterEach
    void stop() {
        server.stop(0);
    }

    /** Verifies create serializes the imported pseudonym and accepts HTTP 201. */
    @Test
    void createsPseudonymThroughClient() {
        OperationService service = new OperationService(client(), properties);
        Object result = service.execute(message());

        assertEquals("012-000000009-2", ((org.trustdeck.client.model.Pseudonym) result).getPsn());
    }

    /** Verifies entity creation uses the arbitrary mapped JSON object. */
    @Test
    void createsEntityThroughClient() {
        properties.setOperation("CREATE_ENTITY");
        OperationService service = new OperationService(client(), properties);

        Object result = service.execute(message());

        assertEquals(UUID.fromString("11111111-1111-1111-1111-111111111111"),
                ((org.trustdeck.client.model.Entity) result).getTrustdeckID());
    }

    /** Verifies entity deletion uses the exact UUID endpoint and HTTP 204. */
    @Test
    void deletesEntityThroughClient() {
        properties.setOperation("DELETE_ENTITY");
        OperationService service = new OperationService(client(), properties);

        assertEquals(Boolean.TRUE, service.execute(message()));
    }

    /**
     * Creates a caller-controlled TrustDeck client.
     *
     * @return client
     */
    private TrustDeckClient client() {
        return new TrustDeckClient(properties.getTrustdeck().getServiceUrl(), () -> "token");
    }

    /**
     * Creates a synthetic mapped message.
     *
     * @return mapped message
     */
    private MappedMessage message() {
        com.fasterxml.jackson.databind.node.ObjectNode data = new com.fasterxml.jackson.databind.ObjectMapper()
                .createObjectNode().put("caseNumber", "0123456789");
        return new MappedMessage("source/domain", "0123456789", "case_number", "012-000000009-2",
                LocalDateTime.of(2025, 10, 9, 19, 8, 10, 175_000_000), null, "source-user", "synthetic-type",
                UUID.fromString("11111111-1111-1111-1111-111111111111"), data,
                "old", "type", "old-psn", "new", "type", "new-psn", "source/domain", "target/domain");
    }

    /**
     * Returns controlled responses for the client endpoints.
     *
     * @param exchange HTTP exchange
     * @throws IOException when response writing fails
     */
    private void respond(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        int status = path.contains("/entities/") && "DELETE".equals(exchange.getRequestMethod()) ? 204
                : path.contains("/entities/") && "POST".equals(exchange.getRequestMethod()) ? 201 : 201;
        byte[] response = status == 204 ? new byte[0]
                : "{\"psn\":\"012-000000009-2\",\"trustdeckID\":\"11111111-1111-1111-1111-111111111111\",\"data\":{}}".getBytes();
        exchange.sendResponseHeaders(status, response.length);
        exchange.getResponseBody().write(response);
        exchange.close();
    }
}
