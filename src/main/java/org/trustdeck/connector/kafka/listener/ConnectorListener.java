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

package org.trustdeck.connector.kafka.listener;

import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.trustdeck.client.exception.RecordLinkageConflictException;
import org.trustdeck.client.exception.TrustDeckResponseException;
import org.trustdeck.connector.kafka.config.ConnectorProperties;
import org.trustdeck.connector.kafka.health.OutageCoordinator;
import org.trustdeck.connector.kafka.mapping.JsonMessageMapper;
import org.trustdeck.connector.kafka.mapping.MappedMessage;
import org.trustdeck.connector.kafka.mapping.MappingException;
import org.trustdeck.connector.kafka.operation.OperationService;
import org.trustdeck.connector.kafka.operation.PseudonymBatchService;
import org.trustdeck.connector.kafka.recovery.RecoveryPublisher;
import org.trustdeck.connector.kafka.statistics.ConnectorStatistics;
import org.trustdeck.connector.kafka.trustdeck.TrustDeckPreflight;

/**
 * Consumes Kafka source records and dispatches them to the configured TrustDeck operation.
 *
 * The listener maps incoming records, verifies dynamic TrustDeck targets, handles
 * batch pseudonym creation where safe, retries transient failures, publishes
 * unrecoverable records to recovery topics, and pauses consumption for systemic
 * failures. Source offsets are acknowledged only after successful processing or
 * confirmed recovery publication.
 *
 * Systemic failures and exhausted batch time budgets do not throw. Instead the
 * listener negatively acknowledges the first unprocessed record: Spring Kafka
 * commits the records before it and seeks the remaining partitions back, so they
 * are redelivered by a later poll. The consumer keeps polling (paused, if an
 * outage was detected), which keeps it inside {@code max.poll.interval.ms} and
 * in its consumer group.
 *
 * @author Armin Müller
 */
@Slf4j
@Component
public class ConnectorListener {

	/** Maps raw Kafka values to connector messages. */
	private final JsonMessageMapper mapper;

	/** Dispatches mapped messages to the configured TrustDeck operation. */
	private final OperationService operations;

	/** Publishes failed or ambiguous records to recovery topics. */
	private final RecoveryPublisher recovery;

	/** Connector configuration controlling processing and recovery behavior. */
	private final ConnectorProperties properties;

	/** Coordinates safe batch execution for pseudonym creation. */
	private final PseudonymBatchService batches;

	/** Pauses source consumption when a systemic failure is detected. */
	private final OutageCoordinator outage;

	/** Performs authenticated preflight checks for static and dynamic TrustDeck resources. */
	private final TrustDeckPreflight preflight;

	/** Collects optional asynchronous runtime and backend statistics. */
	private final ConnectorStatistics statistics;

	/**
	 * Creates the listener without resource preflight or statistics collection.
	 *
	 * @param mapper source message mapper
	 * @param operations TrustDeck operation dispatcher
	 * @param recovery recovery publisher
	 * @param properties connector configuration
	 * @param batches pseudonym batch coordinator
	 * @param outage systemic failure coordinator
	 */
    public ConnectorListener(JsonMessageMapper mapper, OperationService operations, RecoveryPublisher recovery,
    				ConnectorProperties properties, PseudonymBatchService batches, OutageCoordinator outage) {
        this(mapper, operations, recovery, properties, batches, outage, null, null);
    }

    /**
	 * Creates the listener with authenticated resource preflight but without
	 * statistics collection.
	 *
	 * @param mapper source message mapper
	 * @param operations TrustDeck operation dispatcher
	 * @param recovery recovery publisher
	 * @param properties connector configuration
	 * @param batches pseudonym batch coordinator
	 * @param outage systemic failure coordinator
	 * @param preflight authenticated TrustDeck resource preflight
	 */
    public ConnectorListener(JsonMessageMapper mapper, OperationService operations, RecoveryPublisher recovery,
    				ConnectorProperties properties, PseudonymBatchService batches, OutageCoordinator outage,
    				TrustDeckPreflight preflight) {
        this(mapper, operations, recovery, properties, batches, outage, preflight, null);
    }

    /**
	 * Creates the listener with authenticated resource preflight and optional
	 * asynchronous statistics collection.
	 *
	 * @param mapper source message mapper
	 * @param operations TrustDeck operation dispatcher
	 * @param recovery recovery publisher
	 * @param properties connector configuration
	 * @param batches pseudonym batch coordinator
	 * @param outage systemic failure coordinator
	 * @param preflight authenticated TrustDeck resource preflight
	 * @param statistics optional statistics collector
	 */
    @Autowired
    public ConnectorListener(JsonMessageMapper mapper, OperationService operations, RecoveryPublisher recovery,
    				ConnectorProperties properties, PseudonymBatchService batches, OutageCoordinator outage,
    				TrustDeckPreflight preflight, ConnectorStatistics statistics) {
        this.mapper = mapper;
        this.operations = operations;
        this.recovery = recovery;
        this.properties = properties;
        this.batches = batches;
        this.outage = outage;
        this.preflight = preflight;
        this.statistics = statistics;
    }

    /**
	 * Processes one batch of source records received from Kafka.
	 *
	 * Records are mapped individually before processing. Safe pseudonym creation
	 * batches are submitted together; all other records are processed individually.
	 * Offsets are acknowledged only after successful processing or confirmed
	 * publication to the appropriate recovery topic.
	 *
	 * When a systemic failure pauses consumption, or the batch time budget is
	 * exhausted, the first unprocessed record is negatively acknowledged so it and
	 * all later records are redelivered.
	 *
	 * @param records source records from one Kafka poll
	 * @param acknowledgment manual batch acknowledgment
	 * @throws InterruptedException if a retry delay is interrupted, typically during shutdown
	 */
    @KafkaListener(topics = "${connector.source-topic}", groupId = "${connector.group-id}")
    public void onMessage(List<ConsumerRecord<byte[], byte[]>> records, Acknowledgment acknowledgment) throws InterruptedException {
        long started = System.nanoTime();
        long deadline = started + properties.getRecovery().getBatchTimeBudget().toNanos();

        try {
			process(records, acknowledgment, deadline);
		} catch (Redeliver redeliver) {
			log.warn("Redelivering {} of {} record(s) starting at {}: {}", records.size() - redeliver.index, records.size(),
					correlation(records.get(redeliver.index)), redeliver.getMessage());
			acknowledgment.nack(redeliver.index, Duration.ZERO);
		} finally {
			// Record batch timing even when processing exits early or fails
			if (statistics != null) {
				statistics.recordBatch(records.size(), System.nanoTime() - started);
			}
		}
    }

    /**
	 * Maps and processes one batch.
	 *
	 * @param records source records from one Kafka poll
	 * @param acknowledgment manual batch acknowledgment
	 * @param deadline {@link System#nanoTime()} value after which no further record or retry is started
	 * @throws Redeliver if processing must stop and the remaining records must be redelivered
	 * @throws InterruptedException if a retry delay is interrupted
	 */
    private void process(List<ConsumerRecord<byte[], byte[]>> records, Acknowledgment acknowledgment, long deadline)
    				throws Redeliver, InterruptedException {
		List<MappedMessage> mapped = new ArrayList<>();
		List<MappingException> invalid = new ArrayList<>();
		List<String> invalidCategory = new ArrayList<>();

		// Map all source records first while preserving their original batch positions
		for (ConsumerRecord<byte[], byte[]> record : records) {
			MappedMessage message = null;
			MappingException failure = null;
			String category = null;

			if (record.value() == null || record.value().length > properties.getMaxRecordBytes()) {
				failure = new MappingException("Kafka value is null or exceeds the configured size.");
				category = "INVALID_RECORD";
			} else {
				try {
					message = mapper.map(record.value());
				} catch (MappingException exception) {
					failure = exception;
					category = "INVALID_MAPPING";
				}
			}

			mapped.add(message);
			invalid.add(failure);
			invalidCategory.add(category);
		}

		// Process eligible pseudonym-create polls as one correlated TrustDeck batch
		if (properties.getOperation().equals("CREATE_PSEUDONYM") && safeBatch(records, mapped)) {
			processBatch(records, mapped, acknowledgment);
			return;
		}

		// Process records individually; invalid records are published to the DLT in input order
		for (int index = 0; index < records.size(); index++) {
			if (mapped.get(index) == null) {
				// Invalid records were not published during mapping, so a budget stop cannot duplicate DLT entries
				publishDlt(records.get(index), null, invalidCategory.get(index), 0, invalid.get(index), index);
				acknowledgment.acknowledge(index);
				continue;
			}

			// Leave remaining work to the next poll instead of risking max.poll.interval.ms
			if (index > 0 && System.nanoTime() >= deadline) {
				throw new Redeliver(index, "batch time budget exhausted");
			}

			processOne(records.get(index), mapped.get(index), acknowledgment, index, deadline);
		}
    }

    /**
	 * Processes a safe pseudonym-create batch as one TrustDeck request.
	 *
	 * @param records source records from one Kafka poll
	 * @param mapped corresponding mapped messages, none of them {@code null}
	 * @param acknowledgment manual batch acknowledgment
	 * @throws Redeliver if a systemic failure paused consumption or recovery publication failed
	 */
    private void processBatch(List<ConsumerRecord<byte[], byte[]>> records, List<MappedMessage> mapped,
    				Acknowledgment acknowledgment) throws Redeliver {
		try {
			verifyTargets(mapped);
			batches.execute(mapped);
		} catch (Exception failure) {
			if (FailureClassifier.systemic(failure)) {
				pause(failure, failedDomain(mapped), failedEntityType(mapped));
				throw new Redeliver(0, failure);
			}

			boolean unknown = failure instanceof IllegalStateException;

			// Publish and acknowledge each source record according to the batch failure outcome
			for (int index = 0; index < records.size(); index++) {
				if (unknown) {
					publishUnknown(records.get(index), mapped.get(index), failure,
							operations.completedUuid(correlation(records.get(index))), index);
				} else {
					publishDlt(records.get(index), mapped.get(index), FailureClassifier.category(failure), 0, failure, index);
				}
				acknowledgment.acknowledge(index);
			}

			return;
		}

		acknowledgment.acknowledge();
    }

    /**
	 * Processes one mapped source record while preserving its Kafka offset contract.
	 *
	 * Successful records and records successfully published for review or recovery
	 * are acknowledged individually. Systemic or unpublishable failures remain unacknowledged.
	 *
	 * @param record original Kafka source record
	 * @param mapped mapped connector message
	 * @param acknowledgment batch acknowledgment
	 * @param index index of the record within the current batch
	 * @param deadline batch deadline as a {@link System#nanoTime()} value
	 * @throws Redeliver if processing must stop at this record
	 * @throws InterruptedException if a retry delay is interrupted
	 */
    private void processOne(ConsumerRecord<byte[], byte[]> record, MappedMessage mapped, Acknowledgment acknowledgment,
    				int index, long deadline) throws Redeliver, InterruptedException {
        try {
            verifyTargets(List.of(mapped));
            operations.execute(mapped, correlation(record));

            // Future result publishing belongs here, with the source correlation and returned object.
            acknowledgment.acknowledge(index);
        } catch (RecordLinkageConflictException exception) {
            publishReview(record, mapped, 1, exception, index);
            acknowledgment.acknowledge(index);
        } catch (MappingException | IllegalArgumentException exception) {
            publishDlt(record, mapped, "INVALID_MAPPING", 0, exception, index);
            acknowledgment.acknowledge(index);
        } catch (Exception exception) {
            retryOrDlt(record, mapped, exception, acknowledgment, index, deadline);
        }
    }

    /**
	 * Performs authenticated preflight checks for dynamic TrustDeck targets.
	 *
	 * Pseudonym operations verify the resolved domain, while entity operations
	 * verify the resolved project entity type. No checks are performed when
	 * preflight validation is disabled.
	 *
	 * @param mapped messages whose dynamic targets should be verified
	 */
    private void verifyTargets(List<MappedMessage> mapped) {
        if (preflight == null) {
            return;
        }

        for (MappedMessage message : mapped) {
            if (message == null) {
                continue;
            }

            String operation = properties.getOperation();
            if (operation.equals("CREATE_PSEUDONYM") || operation.equals("UPDATE_PSEUDONYM")
                    || operation.equals("DELETE_PSEUDONYM") || operation.equals("CREATE_ENTITY_AND_PSEUDONYM")) {
                preflight.verifyDomain(message.domain());
            }

            if (operation.equals("CREATE_ENTITY") || operation.equals("UPDATE_ENTITY")
                    || operation.equals("DELETE_ENTITY") || operation.equals("CREATE_ENTITY_AND_PSEUDONYM")) {
                preflight.verifyEntityType(properties.getTrustdeck().getProject(), message.entityType());
            }
        }
    }

    /**
	 * Determines whether the current poll can be processed as one pseudonym batch.
	 *
	 * A batch is considered safe only when every record was mapped successfully,
	 * belongs to the same Kafka partition and TrustDeck domain, contains a
	 * pseudonym value, and uses identical validity settings.
	 *
	 * @param records source records from the current poll
	 * @param mappedMessages corresponding mapped messages
	 * @return {@code true} if the records can be correlated and processed safely as one batch, otherwise {@code false}
	 */
    private boolean safeBatch(List<ConsumerRecord<byte[], byte[]>> records, List<MappedMessage> mappedMessages) {
        if (records.isEmpty() || mappedMessages.stream().anyMatch(Objects::isNull)) {
            return false;
        }

        int partition = records.getFirst().partition();
        MappedMessage first = mappedMessages.getFirst();
        String domain = first.domain();

        return records.stream().allMatch(record -> record.partition() == partition)
                && mappedMessages.stream().allMatch(message -> domain.equals(message.domain()) && message.psn() != null
                        && Objects.equals(first.validityTime(), message.validityTime())
                        && Objects.equals(first.validFrom(), message.validFrom())
                        && Objects.equals(first.validTo(), message.validTo()));
    }

    /**
	 * Retries a record-local operation using bounded delays before publishing the
	 * record to the dead-letter topic.
	 *
	 * Systemic failures stop retry processing immediately and pause source
	 * consumption. A successfully retried or successfully dead-lettered record is
	 * acknowledged individually. If the next retry would start after the batch
	 * deadline, the record is redelivered by a later poll instead; the first record
	 * of a batch is exempt so every poll makes progress.
	 *
	 * @param record original Kafka source record
	 * @param mapped mapped connector message
	 * @param failure initial operation failure
	 * @param acknowledgment batch acknowledgment
	 * @param index index of the record within the current batch
	 * @param deadline batch deadline as a {@link System#nanoTime()} value
	 * @throws Redeliver if a systemic failure occurs, the batch deadline is reached, or DLT publication cannot be confirmed
	 * @throws InterruptedException if a retry delay is interrupted
	 */
    private void retryOrDlt(ConsumerRecord<byte[], byte[]> record, MappedMessage mapped, Exception failure,
    				Acknowledgment acknowledgment, int index, long deadline) throws Redeliver, InterruptedException {

    	if (FailureClassifier.systemic(failure)) {
            pause(failure, mapped.domain(), mapped.entityType());
            throw new Redeliver(index, failure);
        }

    	// Attempt to redo the operation
    	int attempts = properties.getRecovery().getMaxAttempts();
        for (int attempt = 1; attempt <= attempts; attempt++) {
            long delay = retryDelay(failure, attempt);
            if (index > 0 && System.nanoTime() + Duration.ofMillis(delay).toNanos() >= deadline) {
                throw new Redeliver(index, "batch time budget exhausted during retries");
            }

            try {
                Thread.sleep(delay);
                operations.execute(mapped, correlation(record));
                acknowledgment.acknowledge(index);
                return;
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw exception;
            } catch (Exception retryFailure) {
                // Failed again
            	failure = retryFailure;

                if (FailureClassifier.systemic(retryFailure)) {
                    pause(retryFailure, mapped.domain(), mapped.entityType());
                    throw new Redeliver(index, retryFailure);
                }
            }
        }

        // After the configured amount of retries, the operation was still unsuccessful --> push the message to the DLT
        publishDlt(record, mapped, FailureClassifier.category(failure), attempts, failure, index);
        acknowledgment.acknowledge(index);
    }

    /**
	 * Determines the delay before the next retry attempt.
	 *
	 * A valid server-provided {@code Retry-After} value takes precedence. Otherwise
	 * an exponential delay based on the configured initial backoff is used. Both
	 * forms are bounded to a maximum of 30 seconds.
	 *
	 * @param failure failure that triggered the retry
	 * @param attempt current retry attempt starting at one
	 * @return retry delay in milliseconds
	 */
    private long retryDelay(Exception failure, int attempt) {
        if (failure instanceof TrustDeckResponseException response) {
            Long retryAfter = retryAfterMillis(response.getRetryAfter());
            if (retryAfter != null) {
                return retryAfter;
            }
        }

        return Math.min(Duration.ofSeconds(30).toMillis(), properties.getRecovery().getInitialBackoff().toMillis() * (1L << Math.min(attempt - 1, 5)));
    }

    /**
	 * Parses an HTTP {@code Retry-After} value into a bounded retry delay.
	 *
	 * Both delta-seconds and RFC 1123 HTTP dates are accepted. Invalid or negative
	 * values are ignored, and valid delays are limited to 30 seconds.
	 *
	 * @param value Retry-After header value
	 * @return bounded delay in milliseconds, or {@code null} if the value is invalid
	 */
    private Long retryAfterMillis(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }

        String header = value.trim();
        try {
            long seconds = Long.parseLong(header);
            if (seconds < 0) {
                return null;
            }

            return seconds >= 30 ? Duration.ofSeconds(30).toMillis() : seconds * 1000L;
        } catch (NumberFormatException ignored) {
            try {
                long millis = Duration.between(Instant.now(), ZonedDateTime.parse(header, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant()).toMillis();

                return Math.max(0, Math.min(Duration.ofSeconds(30).toMillis(), millis));
            } catch (RuntimeException invalidHeader) {
                return null;
            }
        }
    }

    /**
	 * Pauses source consumption because of a systemic failure.
	 *
	 * @param failure systemic failure
	 * @param domain failed dynamic domain, or {@code null}
	 * @param entityType failed dynamic entity type, or {@code null}
	 */
    private void pause(Throwable failure, String domain, String entityType) {
        log.warn("Systemic failure, pausing consumption: {}", FailureClassifier.describe(failure));
        outage.pause(FailureClassifier.category(failure), domain, entityType, properties.getTrustdeck().getProject());
    }

    /**
	 * Publishes a failed record to the dead-letter topic.
	 *
	 * If recovery publication itself fails, source consumption is paused and the
	 * source record remains unacknowledged.
	 *
	 * @param record original Kafka source record
	 * @param mapped mapped connector message, if available
	 * @param category sanitized recovery category
	 * @param attempts number of completed retry attempts
	 * @param failure failure that caused recovery publication
	 * @param index index of the record within the current batch
	 * @throws Redeliver if dead-letter publication cannot be confirmed
	 */
    private void publishDlt(ConsumerRecord<byte[], byte[]> record, MappedMessage mapped, String category, int attempts,
    				Throwable failure, int index) throws Redeliver {
        try {
            recovery.publishDlt(record, mapped, category, attempts, failure);
        } catch (Exception publicationFailure) {
            throw recoveryUnavailable(index, publicationFailure);
        }
    }

    /**
	 * Publishes a linkage conflict to the review topic.
	 *
	 * If review publication itself fails, source consumption is paused and the
	 * source record remains unacknowledged.
	 *
	 * @param record original Kafka source record
	 * @param mapped mapped connector message
	 * @param attempts number of completed processing attempts
	 * @param failure linkage conflict that caused review publication
	 * @param index index of the record within the current batch
	 * @throws Redeliver if review publication cannot be confirmed
	 */
    private void publishReview(ConsumerRecord<byte[], byte[]> record, MappedMessage mapped, int attempts, Throwable failure,
    				int index) throws Redeliver {
        try {
            recovery.publishReview(record, mapped, attempts, failure);
        } catch (Exception publicationFailure) {
            throw recoveryUnavailable(index, publicationFailure);
        }
    }

    /**
	 * Publishes a record whose TrustDeck operation outcome cannot be determined.
	 *
	 * A completed UUID is included when available so later recovery can distinguish
	 * a completed operation from one that still requires replay. Publication
	 * failure pauses source consumption.
	 *
	 * @param record original Kafka source record
	 * @param mapped mapped connector message
	 * @param failure failure that left the operation outcome uncertain
	 * @param completedUuid completed TrustDeck object UUID, if known
	 * @param index index of the record within the current batch
	 * @throws Redeliver if recovery publication cannot be confirmed
	 */
    private void publishUnknown(ConsumerRecord<byte[], byte[]> record, MappedMessage mapped, Throwable failure,
    				UUID completedUuid, int index) throws Redeliver {
        try {
            recovery.publishUnknown(record, mapped, failure, completedUuid);
        } catch (Exception publicationFailure) {
            throw recoveryUnavailable(index, publicationFailure);
        }
    }

    /**
	 * Pauses consumption because the recovery producer is unavailable.
	 *
	 * @param index index of the record whose recovery publication failed
	 * @param publicationFailure publication failure
	 * @return redelivery signal starting at the affected record
	 */
    private Redeliver recoveryUnavailable(int index, Exception publicationFailure) {
        log.warn("Recovery publication failed, pausing consumption: {}", FailureClassifier.describe(publicationFailure));
        outage.pause(FailureClassifier.RECOVERY_PRODUCER_UNAVAILABLE);

        return new Redeliver(index, publicationFailure);
    }

    /**
	 * Returns the domain associated with a failed batch.
	 *
	 * @param mapped mapped batch messages
	 * @return first mapped domain, or {@code null} if none is available
	 */
    private String failedDomain(List<MappedMessage> mapped) {
        return mapped.isEmpty() || mapped.getFirst() == null ? null : mapped.getFirst().domain();
    }

    /**
	 * Returns the entity type associated with a failed batch.
	 *
	 * @param mapped mapped batch messages
	 * @return first mapped entity type, or {@code null} if none is available
	 */
    private String failedEntityType(List<MappedMessage> mapped) {
        return mapped.isEmpty() || mapped.getFirst() == null ? null : mapped.getFirst().entityType();
    }

    /**
	 * Builds a deterministic correlation identifier for a source record.
	 *
	 * The correlation combines topic, partition, and offset so combined-stage
	 * operations can identify the same source record during bounded replay.
	 *
	 * @param record source record
	 * @return deterministic source correlation
	 */
    private String correlation(ConsumerRecord<byte[], byte[]> record) {
        return record.topic() + "/" + record.partition() + "/" + record.offset();
    }

    /**
	 * Signals that batch processing stops at a record that must be redelivered,
	 * together with all later records of the batch.
	 */
    private static final class Redeliver extends Exception {

		private static final long serialVersionUID = 1L;

		/** Index of the first record that was neither processed nor acknowledged. */
		private final int index;

		/**
		 * Creates a redelivery signal with a sanitized reason.
		 *
		 * @param index first unprocessed record
		 * @param reason sanitized reason
		 */
		Redeliver(int index, String reason) {
			super(reason, null, false, false);
			this.index = index;
		}

		/**
		 * Creates a redelivery signal caused by a failure.
		 *
		 * @param index first unprocessed record
		 * @param cause failure that stopped processing
		 */
		Redeliver(int index, Throwable cause) {
			this(index, FailureClassifier.describe(cause));
		}
    }
}
