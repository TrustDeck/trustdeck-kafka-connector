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

import java.util.List;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.trustdeck.client.TrustDeckClient;
import org.trustdeck.client.model.BatchResult;
import org.trustdeck.client.model.IdentifierItem;
import org.trustdeck.client.model.Pseudonym;
import org.trustdeck.connector.kafka.config.ConnectorProperties;
import org.trustdeck.connector.kafka.mapping.MappedMessage;

/** Uses the TrustDeck create-batch endpoint only when response correlation is unambiguous. */
@Service
public class PseudonymBatchService {

    /** Shared TrustDeck client. */
    private final TrustDeckClient client;

    /** Batch size setting. */
    private final ConnectorProperties properties;

    /** JSON serializer used for enforcing the wire-size limit. */
    private final ObjectMapper mapper;

    /** Creates the batch service with an application JSON mapper.
     * @param client TrustDeck client
     * @param properties connector settings
     * @param mapper configured JSON mapper
     */
    public PseudonymBatchService(TrustDeckClient client, ConnectorProperties properties, ObjectMapper mapper) {
        this.client = client;
        this.properties = properties;
        this.mapper = mapper;
    }

    /**
     * Creates a safe batch for messages with imported values and one domain.
     *
     * @param messages compatible mapped messages
     * @return returned pseudonyms
     */
    public List<Pseudonym> execute(List<MappedMessage> messages) {
        if (messages.isEmpty()) {
            return List.of();
        }
        String domain = messages.getFirst().domain();
        if (messages.size() > properties.getBatchSize() || messages.stream()
                .anyMatch(message -> !domain.equals(message.domain()) || message.psn() == null)) {
            throw new IllegalArgumentException("Pseudonym batch contains incompatible records.");
        }
        List<Pseudonym> request = messages.stream().map(message -> Pseudonym.builder()
                .identifierItem(IdentifierItem.builder().identifier(message.identifier()).idType(message.idType()).build())
                .psn(message.psn()).validityTime(message.validityTime()).validFrom(message.validFrom())
                .validTo(message.validTo()).build()).toList();
        try {
            if (mapper.writeValueAsBytes(request).length > properties.getBatchBytes()) {
                throw new IllegalArgumentException("Pseudonym batch exceeds the configured byte limit.");
            }
        } catch (com.fasterxml.jackson.core.JsonProcessingException exception) {
            throw new IllegalArgumentException("Pseudonym batch cannot be serialized.", exception);
        }
        BatchResult<Pseudonym> result = client.pseudonyms(domain).createBatch(request, false);
        Map<String, Queue<Integer>> occurrences = new HashMap<>();
        for (int index = 0; index < messages.size(); index++) {
            occurrences.computeIfAbsent(key(messages.get(index)), ignored -> new ArrayDeque<>()).add(index);
        }
        List<Pseudonym> correlated = new ArrayList<>(messages.size());
        for (Pseudonym returned : result.items()) {
            if (returned == null || returned.getIdentifierItem() == null) {
                throw new IllegalStateException("Pseudonym batch response correlation is ambiguous.");
            }
            Queue<Integer> indexes = occurrences.get(key(returned));
            if (indexes == null || indexes.isEmpty()) {
                throw new IllegalStateException("Pseudonym batch response correlation is ambiguous.");
            }
            indexes.remove();
            correlated.add(returned);
        }
        if (!result.partial() && occurrences.values().stream().anyMatch(queue -> !queue.isEmpty())) {
            throw new IllegalStateException("Pseudonym batch response omitted an input record.");
        }
        if (result.partial()) {
            if (correlated.isEmpty()) {
                throw new IllegalStateException("Partial pseudonym response has no safely correlated successes.");
            }
            for (Queue<Integer> unresolved : occurrences.values()) {
                while (!unresolved.isEmpty()) {
                    int index = unresolved.remove();
                    correlated.add(client.pseudonyms(domain).create(request.get(index), false));
                }
            }
        }
        return correlated;
    }

    /**
     * Builds an occurrence-matching key from returned complete identifying fields.
     *
     * @param pseudonym pseudonym value
     * @return correlation key
     */
    private String key(Pseudonym pseudonym) {
        return pseudonym.getIdentifierItem().getIdentifier() + "\u0000"
                + pseudonym.getIdentifierItem().getIdType() + "\u0000" + pseudonym.getPsn();
    }

    /**
     * Builds an occurrence-matching key from source fields.
     *
     * @param message mapped source
     * @return correlation key
     */
    private String key(MappedMessage message) {
        return message.identifier() + "\u0000" + message.idType() + "\u0000" + message.psn();
    }
}
