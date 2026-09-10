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

    /**
     * Creates the batch service.
     *
     * @param client TrustDeck client
     * @param properties connector settings
     */
    public PseudonymBatchService(TrustDeckClient client, ConnectorProperties properties) {
        this.client = client;
        this.properties = properties;
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
                .psn(message.psn()).validFrom(message.validFrom()).validTo(message.validTo()).build()).toList();
        BatchResult<Pseudonym> result = client.pseudonyms(domain).createBatch(request, false);
        if (result.items().size() != messages.size()) {
            throw new IllegalStateException("Pseudonym batch response is partial or cannot be correlated safely.");
        }
        for (int index = 0; index < messages.size(); index++) {
            Pseudonym returned = result.items().get(index);
            MappedMessage source = messages.get(index);
            if (returned == null || returned.getIdentifierItem() == null
                    || !source.identifier().equals(returned.getIdentifierItem().getIdentifier())
                    || !source.idType().equals(returned.getIdentifierItem().getIdType())
                    || !source.psn().equals(returned.getPsn())) {
                throw new IllegalStateException("Pseudonym batch response correlation is ambiguous.");
            }
        }
        return result.items();
    }
}
