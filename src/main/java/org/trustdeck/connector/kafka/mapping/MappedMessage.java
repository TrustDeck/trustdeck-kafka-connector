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

package org.trustdeck.connector.kafka.mapping;

import java.time.LocalDateTime;
import java.util.UUID;

import com.fasterxml.jackson.databind.node.ObjectNode;

/** Immutable result of mapping one source JSON object to connector fields.
 * @param domain resolved domain
 * @param identifier resolved identifier
 * @param idType resolved identifier type
 * @param psn resolved pseudonym
 * @param validFrom validity start
 * @param validTo validity end
 * @param username source user
 * @param entityType entity type
 * @param entityId entity UUID
 * @param entityData entity payload
 * @param oldIdentifier old identifier
 * @param oldIdType old identifier type
 * @param oldPsn old pseudonym
 * @param newIdentifier replacement identifier
 * @param newIdType replacement identifier type
 * @param newPsn replacement pseudonym
 * @param oldDomain old domain
 * @param newDomain replacement domain
 */
public record MappedMessage(String domain, String identifier, String idType, String psn,
                            LocalDateTime validFrom, LocalDateTime validTo, String username,
                            String entityType, UUID entityId, ObjectNode entityData,
                            String oldIdentifier, String oldIdType, String oldPsn,
                            String newIdentifier, String newIdType, String newPsn,
                            String oldDomain, String newDomain) {
}
