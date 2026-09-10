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

package org.trustdeck.connector.kafka.kafka;

import java.time.Instant;
import java.util.Map;

/** Versioned, replayable recovery record without credentials or arbitrary headers.
 * @param schemaVersion envelope schema version
 * @param status terminal status
 * @param sourceTopic source topic
 * @param partition source partition
 * @param offset source offset
 * @param sourceTimestamp source timestamp
 * @param operation operation name
 * @param resolvedTarget resolved target
 * @param failureCategory safe failure category
 * @param httpStatus HTTP status when available
 * @param attempts attempt count
 * @param completedStage completed combined-operation stage
 * @param completedUuid completed entity UUID
 * @param keyBase64 original key
 * @param valueBase64 original value
 * @param username source user
 * @param safeHeaders allowlisted source headers
 */
public record RecoveryEnvelope(String schemaVersion, String status, String sourceTopic, int partition,
                               long offset, Instant sourceTimestamp, String operation, String resolvedTarget,
                               String failureCategory, Integer httpStatus, int attempts,
                                String completedStage, String completedUuid, String keyBase64,
                                String valueBase64, String username, Map<String, String> safeHeaders) {
}
