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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Service;
import org.trustdeck.client.TrustDeckClient;
import org.trustdeck.client.model.Entity;
import org.trustdeck.client.model.IdentifierItem;
import org.trustdeck.client.model.Pseudonym;
import org.trustdeck.client.model.PseudonymUpdate;
import org.trustdeck.connector.kafka.config.ConnectorProperties;
import org.trustdeck.connector.kafka.mapping.MappedMessage;

/** Dispatches mapped records to the exact TrustDeck resource operation. */
@Service
public class OperationService {

    /** Shared TrustDeck client. */
    private final TrustDeckClient client;

    /** Connector operation settings. */
    private final ConnectorProperties properties;

    /** Bounded completed first stages for combined-operation retries. */
    private final Map<String, UUID> combinedStages = new LinkedHashMap<>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, UUID> eldest) {
            return size() > 10_000;
        }
    };

    /**
     * Creates the operation service.
     *
     * @param client shared TrustDeck client
     * @param properties connector settings
     */
    public OperationService(TrustDeckClient client, ConnectorProperties properties) {
        this.client = client;
        this.properties = properties;
    }

    /**
     * Executes one mapped operation synchronously.
     *
     * @param message mapped input
     * @return returned TrustDeck object, when one exists
     */
    public Object execute(MappedMessage message) {
        return execute(message, null);
    }

    /**
     * Executes one operation and uses a source correlation for combined-stage replay.
     *
     * @param message mapped input
     * @param correlation stable source topic/partition/offset correlation, or null
     * @return returned TrustDeck object
     */
    public Object execute(MappedMessage message, String correlation) {
        return switch (properties.getOperation()) {
            case "CREATE_PSEUDONYM" -> createPseudonym(message);
            case "UPDATE_PSEUDONYM" -> updatePseudonym(message);
            case "DELETE_PSEUDONYM" -> deletePseudonym(message);
            case "CREATE_ENTITY" -> createEntity(message);
            case "UPDATE_ENTITY" -> updateEntity(message);
            case "DELETE_ENTITY" -> deleteEntity(message);
            case "CREATE_ENTITY_AND_PSEUDONYM" -> createEntityAndPseudonym(message, correlation);
            default -> throw new IllegalStateException("Unsupported connector operation.");
        };
    }

    /** Returns the bounded completed entity stage for replay metadata.
     * @param correlation source topic/partition/offset correlation
     * @return completed entity UUID, or null
     */
    public UUID completedUuid(String correlation) {
        synchronized (combinedStages) {
            return combinedStages.get(correlation);
        }
    }

    /** Restores a confirmed combined-operation stage for an explicitly initiated replay. */
    public void restoreCompletedUuid(String correlation, UUID completedUuid) {
        if (correlation == null || completedUuid == null) {
            throw new IllegalArgumentException("Replay stage correlation and UUID are required.");
        }
        synchronized (combinedStages) {
            combinedStages.put(correlation, completedUuid);
        }
    }

    /**
     * Creates a pseudonym and forwards only supplied validity values.
     *
     * @param message mapped input
     * @return returned pseudonym
     */
    private Pseudonym createPseudonym(MappedMessage message) {
        Pseudonym pseudonym = pseudonym(message.identifier(), message.idType(), message.psn(),
                message.validFrom(), message.validTo());
        return client.pseudonyms(message.domain()).create(pseudonym, false);
    }

    /**
     * Updates a pseudonym using explicit old and replacement fields.
     *
     * @param message mapped input
     * @return returned pseudonym
     */
    private Pseudonym updatePseudonym(MappedMessage message) {
        IdentifierItem oldItem = item(message.oldIdentifier(), message.oldIdType());
        if (oldItem == null) {
            throw new IllegalArgumentException("Pseudonym update needs the complete old selector.");
        }
        if ((message.newIdentifier() == null) != (message.newIdType() == null)) {
            throw new IllegalArgumentException("Replacement identifier and id type must be mapped together.");
        }
        PseudonymUpdate update = PseudonymUpdate.builder().oldIdentifierItem(oldItem).oldPsn(message.oldPsn())
                .newIdentifierItem(item(message.newIdentifier(), message.newIdType()))
                .newPsn(message.newPsn()).validFrom(message.validFrom()).validTo(message.validTo()).build();
        String domain = message.oldDomain() == null ? message.domain() : message.oldDomain();
        boolean complete = message.newDomain() != null || message.newIdentifier() != null || message.newPsn() != null;
        if (complete) {
            update.setNewDomainName(message.newDomain() == null ? domain : message.newDomain());
            return client.pseudonyms(domain).updateComplete(update, false);
        }
        return client.pseudonyms(domain).update(update);
    }

    /**
     * Deletes one exact pseudonym selector, preferring its value.
     *
     * @param message mapped input
     * @return true after deletion
     */
    private Boolean deletePseudonym(MappedMessage message) {
        if (message.psn() != null) {
            return client.pseudonyms(message.domain()).delete(message.psn());
        }
        if (message.identifier() == null || message.idType() == null) {
            throw new IllegalArgumentException("Pseudonym deletion needs psn or identifier and idType.");
        }
        Pseudonym selected = client.pseudonyms(message.domain()).get(IdentifierItem.builder()
                .identifier(message.identifier()).idType(message.idType()).build());
        if (selected == null || selected.getPsn() == null) {
            throw new IllegalStateException("Identifier lookup returned no deletable pseudonym.");
        }
        return client.pseudonyms(message.domain()).delete(selected.getPsn());
    }

    /**
     * Creates an entity from explicit mapped JSON data.
     *
     * @param message mapped input
     * @return returned entity
     */
    private Entity createEntity(MappedMessage message) {
        return client.entities(properties.getTrustdeck().getProject(), message.entityType())
                .create(Entity.builder().data(message.entityData()).build());
    }

    /**
     * Replaces entity data by its mapped UUID.
     *
     * @param message mapped input
     * @return returned entity
     */
    private Entity updateEntity(MappedMessage message) {
        return client.entities(properties.getTrustdeck().getProject(), message.entityType())
                .update(message.entityId(), Entity.builder().data(message.entityData()).build());
    }

    /**
     * Tombstones an entity by its mapped UUID.
     *
     * @param message mapped input
     * @return true after deletion
     */
    private Boolean deleteEntity(MappedMessage message) {
        return client.entities(properties.getTrustdeck().getProject(), message.entityType())
                .delete(message.entityId());
    }

    /**
     * Creates an entity and then its requested target pseudonym without rollback.
     *
     * @param message mapped input
     * @return created target pseudonym
     */
    private Pseudonym createEntityAndPseudonym(MappedMessage message, String correlation) {
        UUID entityId;
        synchronized (combinedStages) {
            entityId = correlation == null ? null : combinedStages.get(correlation);
        }
        if (entityId == null) {
            Entity entity = createEntity(message);
            if (entity == null || entity.getTrustdeckID() == null) {
                throw new IllegalStateException("Entity creation returned no TrustDeck UUID.");
            }
            entityId = entity.getTrustdeckID();
            if (correlation != null) {
                synchronized (combinedStages) {
                    combinedStages.put(correlation, entityId);
                }
            }
        }

        // The associated-domain read verifies backend automatic pseudonymization before creating a target value.
        List<Pseudonym> associated = client.entities(properties.getTrustdeck().getProject(), message.entityType())
                .getPseudonyms(entityId).items();
        for (Pseudonym existing : associated) {
            IdentifierItem existingItem = existing.getIdentifierItem();
            boolean sameEntity = existingItem != null && entityId.toString().equals(existingItem.getIdentifier())
                    && "TrustDeckID".equals(existingItem.getIdType());
            boolean sameRequestedValue = message.psn() == null || message.psn().equals(existing.getPsn());
            if (sameEntity && message.domain().equals(existing.getDomainName()) && sameRequestedValue) {
                return existing;
            }
        }
        Pseudonym pseudonym = pseudonym(entityId.toString(), "TrustDeckID", message.psn(),
                message.validFrom(), message.validTo());
        return client.pseudonyms(message.domain()).create(pseudonym, false);
    }

    /**
     * Builds a pseudonym payload.
     *
     * @param identifier identifier
     * @param idType type
     * @param psn optional imported value
     * @param validFrom optional start
     * @param validTo optional end
     * @return pseudonym payload
     */
    private Pseudonym pseudonym(String identifier, String idType, String psn,
                                java.time.LocalDateTime validFrom, java.time.LocalDateTime validTo) {
        return Pseudonym.builder().identifierItem(item(identifier, idType)).psn(psn)
                .validFrom(validFrom).validTo(validTo).build();
    }

    /**
     * Builds an identifier item or null when both values are absent.
     *
     * @param identifier identifier
     * @param idType identifier type
     * @return identifier item
     */
    private IdentifierItem item(String identifier, String idType) {
        return identifier == null && idType == null ? null : IdentifierItem.builder()
                .identifier(identifier).idType(idType).build();
    }
}
