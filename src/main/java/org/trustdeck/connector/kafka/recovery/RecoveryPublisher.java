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

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.Map;
import java.util.Set;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.trustdeck.client.exception.TrustDeckResponseException;
import org.trustdeck.connector.kafka.config.ConnectorProperties;
import org.trustdeck.connector.kafka.mapping.MappedMessage;

/**
 * Publishes recovery envelopes for failed, ambiguous, or review-required records.
 *
 * Recovery data is serialized as JSON and written to the configured DLT or review
 * topic. Publication waits for broker confirmation so the source record is only
 * acknowledged after its recovery envelope has been stored successfully.
 *
 * @author Armin Müller
 */
@Service
public class RecoveryPublisher {

	/** JSON serializer used for recovery envelopes. */
	private final ObjectMapper mapper;

	/** Kafka producer used to publish recovery envelopes. */
	private final KafkaTemplate<String, String> template;

	/** Connector configuration containing recovery topics and operation metadata. */
	private final ConnectorProperties properties;

	/**
	 * Creates the recovery publisher.
	 *
	 * @param template Kafka producer used for recovery topics
	 * @param properties connector configuration
	 * @param mapper JSON mapper used to serialize recovery envelopes
	 */
	public RecoveryPublisher(KafkaTemplate<String, String> template, ConnectorProperties properties, ObjectMapper mapper) {
		this.template = template;
		this.properties = properties;
		this.mapper = mapper;
	}

	/**
	 * Publishes a failed record to the configured dead-letter topic.
	 *
	 * The method waits for broker confirmation before returning so callers can
	 * safely acknowledge the original source record afterwards.
	 *
	 * @param record original Kafka source record
	 * @param mapped mapped connector message, if available
	 * @param category sanitized failure category
	 * @param attempts number of completed processing attempts
	 * @param failure failure that caused dead-letter publication
	 * @throws Exception if serialization or broker-confirmed publication fails
	 */
	public void publishDlt(ConsumerRecord<byte[], byte[]> record, MappedMessage mapped, String category, int attempts, Throwable failure) throws Exception {
		publish(record, mapped, category, "FAILED_DLT", attempts, failure, properties.getRecovery().getDltTopic());
	}

	/**
	 * Publishes a linkage conflict to the configured review topic.
	 *
	 * The envelope is marked for manual linkage review and publication waits for
	 * broker confirmation before returning.
	 *
	 * @param record original Kafka source record
	 * @param mapped mapped connector message
	 * @param attempts number of completed processing attempts
	 * @param failure linkage failure requiring review
	 * @throws Exception if serialization or broker-confirmed publication fails
	 */
	public void publishReview(ConsumerRecord<byte[], byte[]> record, MappedMessage mapped, int attempts, Throwable failure) throws Exception {
		publish(record, mapped, "RECORD_LINKAGE", "PENDING_LINKAGE_REVIEW", attempts, failure,
				properties.getRecovery().getReviewTopic());
	}

	/**
	 * Publishes an envelope for a record whose external operation outcome cannot
	 * be determined safely.
	 *
	 * When the entity stage of a combined operation completed successfully, its
	 * UUID is included so an operator can reconcile the outcome before replaying
	 * the remaining stage.
	 *
	 * @param record original Kafka source record
	 * @param mapped mapped connector message
	 * @param failure failure that left the operation outcome uncertain
	 * @param completedUuid confirmed entity UUID from a completed combined-operation stage
	 * @throws Exception if serialization or broker-confirmed publication fails
	 */
	public void publishUnknown(ConsumerRecord<byte[], byte[]> record, MappedMessage mapped, Throwable failure, UUID completedUuid) throws Exception {
		publish(record, mapped, "UNKNOWN_OUTCOME", "UNKNOWN_OUTCOME", 1, failure, properties.getRecovery().getDltTopic(), 
				completedUuid == null ? null : "Entity stage completed; reconcile the preserved UUID before replaying stage two.", completedUuid);
	}

	/**
	 * Serializes and publishes one recovery envelope without operator guidance.
	 *
	 * @param record original Kafka source record
	 * @param mapped mapped connector message, if available
	 * @param category sanitized failure category
	 * @param status recovery envelope status
	 * @param attempts number of completed processing attempts
	 * @param failure failure associated with the recovery record
	 * @param topic destination recovery topic
	 * @throws Exception if serialization or broker-confirmed publication fails
	 */
	private void publish(ConsumerRecord<byte[], byte[]> record, MappedMessage mapped, String category, String status, 
			int attempts, Throwable failure, String topic) throws Exception {
		publish(record, mapped, category, status, attempts, failure, topic, null);
	}

	/**
	 * Serializes and publishes one recovery envelope with optional operator guidance.
	 *
	 * @param record original Kafka source record
	 * @param mapped mapped connector message, if available
	 * @param category sanitized failure category
	 * @param status recovery envelope status
	 * @param attempts number of completed processing attempts
	 * @param failure failure associated with the recovery record
	 * @param topic destination recovery topic
	 * @param manualGuidance optional operator guidance
	 * @throws Exception if serialization or broker-confirmed publication fails
	 */
	private void publish(ConsumerRecord<byte[], byte[]> record, MappedMessage mapped, String category, String status, 
			int attempts, Throwable failure, String topic, String manualGuidance) throws Exception {
		publish(record, mapped, category, status, attempts, failure, topic, manualGuidance, null);
	}

	/**
	 * Serializes and publishes one recovery envelope with optional completed-stage metadata.
	 *
	 * The original Kafka key and value are preserved as Base64, while only explicitly
	 * allowed provenance headers are copied. Publication waits for broker confirmation
	 * before the method returns.
	 *
	 * @param record original Kafka source record
	 * @param mapped mapped connector message, if available
	 * @param category sanitized failure category
	 * @param status recovery envelope status
	 * @param attempts number of completed processing attempts
	 * @param failure failure associated with the recovery record
	 * @param topic destination recovery topic
	 * @param manualGuidance optional operator guidance
	 * @param completedUuid confirmed entity UUID from a completed combined-operation stage
	 * @throws Exception if serialization, publication, or broker confirmation fails
	 */
	private void publish(ConsumerRecord<byte[], byte[]> record, MappedMessage mapped, String category, String status, 
			int attempts, Throwable failure, String topic, String manualGuidance, UUID completedUuid) throws Exception {
		// Preserve source identity, raw payloads, safe provenance, and resolved replay targets
		RecoveryEnvelope envelope = new RecoveryEnvelope(
				"2",
				status,
				record.topic(),
				record.partition(),
				record.offset(),
				record.timestamp() < 0 ? null : Instant.ofEpochMilli(record.timestamp()),
				properties.getOperation(),
				mapped == null ? null : mapped.domain(),
				category,
				httpStatus(failure),
				attempts,
				completedUuid == null ? null : "ENTITY_CREATED",
				completedUuid == null ? null : completedUuid.toString(),
				record.key() == null ? null : Base64.getEncoder().encodeToString(record.key()),
				Base64.getEncoder().encodeToString(record.value()),
				mapped == null ? null : mapped.username(),
				safeHeaders(record),
				manualGuidance,
				properties.getTrustdeck().getProject(),
				mapped == null ? null : mapped.entityType());

		String json = mapper.writeValueAsString(envelope);
		String key = record.key() == null ? null : Base64.getEncoder().encodeToString(record.key());

		// Wait for broker confirmation so recovery publication is durable before acknowledgment
		template.send(topic, key, json).get(Duration.ofSeconds(10).toMillis(), TimeUnit.MILLISECONDS);
	}

	/**
	 * Extracts the numeric HTTP status from a TrustDeck response failure.
	 *
	 * @param failure failure associated with the recovery record
	 * @return HTTP status code, or {@code null} for non-HTTP failures
	 */
	private Integer httpStatus(Throwable failure) {
		return failure instanceof TrustDeckResponseException e ? e.getStatusCode() : null;
	}

	/**
	 * Copies bounded Kafka headers that are explicitly safe for recovery storage.
	 *
	 * Only allow-listed headers with values up to 256 bytes are retained. When the
	 * same header occurs more than once, the final value is preserved.
	 *
	 * @param record original Kafka source record
	 * @return immutable map containing safe recovery headers
	 */
	private Map<String, String> safeHeaders(ConsumerRecord<byte[], byte[]> record) {
		Set<String> allowed = Set.of("correlation-id", "traceparent", "event-type");

		return Arrays.stream(record.headers().toArray())
				.filter(header -> allowed.contains(header.key())
						&& header.value() != null
						&& header.value().length <= 256)
				.collect(Collectors.toUnmodifiableMap(
						header -> header.key(),
						header -> new String(header.value(), StandardCharsets.UTF_8),
						(first, last) -> last));
	}
}
