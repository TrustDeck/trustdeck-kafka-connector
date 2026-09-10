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

package org.trustdeck.connector.kafka.trustdeck;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;
import org.trustdeck.client.AccessTokenProvider;
import org.trustdeck.connector.kafka.config.ConnectorProperties;

/** Thread-safe OAuth2 client-credentials token provider with expiry skew. */
@Component
public class ClientCredentialsTokenProvider implements AccessTokenProvider {

    /** Refreshes before expiry to avoid using an almost-expired token. */
    private static final Duration EXPIRY_SKEW = Duration.ofSeconds(30);

    /** Token HTTP client. */
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    /** JSON parser. */
    private final ObjectMapper mapper = new ObjectMapper();

    /** OAuth settings. */
    private final ConnectorProperties.TrustDeckProperties properties;

    /** Cached token value. */
    private String token;

    /** Cached token expiry. */
    private Instant expiry = Instant.MIN;

    /**
     * Creates the provider.
     *
     * @param connectorProperties connector settings
     */
    public ClientCredentialsTokenProvider(ConnectorProperties connectorProperties) {
        this.properties = connectorProperties.getTrustdeck();
    }

    /**
     * Returns a cached token or refreshes it once under a lock.
     *
     * @return bearer token without prefix
     */
    @Override
    public synchronized String getAccessToken() {
        if (token != null && Instant.now().isBefore(expiry.minus(EXPIRY_SKEW))) {
            return token;
        }
        try {
            String credentials = properties.getClientId() + ":" + properties.getClientSecret();
            HttpRequest request = HttpRequest.newBuilder(URI.create(properties.getTokenEndpoint()))
                    .timeout(Duration.ofSeconds(30)).header("Authorization", "Basic "
                            + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8)))
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .POST(HttpRequest.BodyPublishers.ofString("grant_type=client_credentials"))
                    .build();
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new IllegalStateException("OAuth token endpoint returned HTTP " + response.statusCode());
            }
            JsonNode body = mapper.readTree(response.body());
            JsonNode accessToken = body.get("access_token");
            if (accessToken == null || !accessToken.isTextual() || accessToken.textValue().isBlank()) {
                throw new IllegalStateException("OAuth token response did not contain access_token.");
            }
            token = accessToken.textValue();
            long expiresIn = body.path("expires_in").asLong(300);
            expiry = Instant.now().plusSeconds(Math.max(1, expiresIn));
            return token;
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("OAuth token acquisition was interrupted.", exception);
        } catch (Exception exception) {
            throw new IllegalStateException("OAuth token acquisition failed.", exception);
        }
    }
}
