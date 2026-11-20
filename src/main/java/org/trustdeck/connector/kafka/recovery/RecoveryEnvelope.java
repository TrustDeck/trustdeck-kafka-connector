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

package org.trustdeck.connector.kafka.recovery;

import java.time.Instant;
import java.util.Map;

/**
 * Immutable recovery record containing the preserved Kafka source data and the
 * metadata required for diagnostics, reconciliation, and explicit replay.
 *
 * The envelope identifies the original Kafka record, the connector operation,
 * the resolved TrustDeck target, the sanitized failure outcome, optional
 * completed-stage metadata, and the Base64-encoded original key and value.
 *
 * @param schemaVersion recovery envelope schema version
 * @param status terminal recovery status
 * @param sourceTopic original Kafka source topic
 * @param partition original Kafka source partition
 * @param offset original Kafka source offset
 * @param sourceTimestamp original Kafka record timestamp
 * @param operation connector operation that processed the record
 * @param resolvedTarget resolved TrustDeck target, typically the domain
 * @param failureCategory sanitized failure category
 * @param httpStatus related HTTP status, if available
 * @param attempts number of completed processing attempts
 * @param completedStage confirmed completed stage of a multi-stage operation, if any
 * @param completedUuid UUID associated with the confirmed completed stage, if any
 * @param keyBase64 Base64-encoded original Kafka key
 * @param valueBase64 Base64-encoded original Kafka value
 * @param username mapped source provenance user, if available
 * @param safeHeaders sanitized Kafka headers safe for recovery storage
 * @param manualGuidance operator guidance for handling the recovery record
 * @param targetProject resolved TrustDeck project, if relevant
 * @param targetEntityType resolved TrustDeck entity type, if relevant
 *
 * @author Armin Müller
 */
public record RecoveryEnvelope(String schemaVersion, String status, String sourceTopic, int partition, long offset, 
		Instant sourceTimestamp, String operation, String resolvedTarget, String failureCategory, Integer httpStatus, 
		int attempts, String completedStage, String completedUuid, String keyBase64, String valueBase64, String username, 
		Map<String, String> safeHeaders, String manualGuidance, String targetProject, String targetEntityType) {

	/**
	 * Creates a recovery envelope without explicit project or entity-type target metadata.
	 *
	 * The project and entity-type target fields are left empty.
	 *
	 * @param schemaVersion recovery envelope schema version
	 * @param status terminal recovery status
	 * @param sourceTopic original Kafka source topic
	 * @param partition original Kafka source partition
	 * @param offset original Kafka source offset
	 * @param sourceTimestamp original Kafka record timestamp
	 * @param operation connector operation that processed the record
	 * @param resolvedTarget resolved TrustDeck target
	 * @param failureCategory sanitized failure category
	 * @param httpStatus related HTTP status, if available
	 * @param attempts number of completed processing attempts
	 * @param completedStage confirmed completed operation stage, if any
	 * @param completedUuid UUID associated with the completed stage, if any
	 * @param keyBase64 Base64-encoded original Kafka key
	 * @param valueBase64 Base64-encoded original Kafka value
	 * @param username mapped source provenance user, if available
	 * @param safeHeaders sanitized Kafka headers
	 * @param manualGuidance operator guidance for handling the recovery record
	 */
	public RecoveryEnvelope(String schemaVersion, String status, String sourceTopic, int partition, long offset, 
			Instant sourceTimestamp, String operation, String resolvedTarget, String failureCategory, Integer httpStatus, 
			int attempts, String completedStage, String completedUuid, String keyBase64, String valueBase64, String username, 
			Map<String, String> safeHeaders, String manualGuidance) {
		
		this(schemaVersion, status, sourceTopic, partition, offset, sourceTimestamp, operation, resolvedTarget,
				failureCategory, httpStatus, attempts, completedStage, completedUuid, keyBase64, valueBase64,
				username, safeHeaders, manualGuidance, null, null);
	}
}
