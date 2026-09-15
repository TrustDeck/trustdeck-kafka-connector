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

import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.trustdeck.client.exception.RecordLinkageConflictException;
import org.trustdeck.connector.kafka.config.ConnectorProperties;
import org.trustdeck.connector.kafka.mapping.JsonMessageMapper;
import org.trustdeck.connector.kafka.mapping.MappedMessage;
import org.trustdeck.connector.kafka.mapping.MappingException;
import org.trustdeck.connector.kafka.operation.OperationService;
import org.trustdeck.connector.kafka.operation.PseudonymBatchService;
import org.trustdeck.connector.kafka.trustdeck.TrustDeckPreflight;

/** Consumes records and acknowledges only completed TrustDeck or recovery work. */
@Component
public class ConnectorListener {

    /** JSON source mapper. */
    private final JsonMessageMapper mapper;

    /** TrustDeck operation dispatcher. */
    private final OperationService operations;

    /** Recovery publisher. */
    private final RecoveryPublisher recovery;

    /** Connector settings. */
    private final ConnectorProperties properties;

    /** Safe pseudonym create batch coordinator. */
    private final PseudonymBatchService batches;

    /** Systemic failure pause coordinator. */
    private final OutageCoordinator outage;

    /** Authenticated static and dynamic resource preflight. */
    private final TrustDeckPreflight preflight;

    /**
     * Creates the listener.
     *
     * @param mapper source mapper
     * @param operations operation service
     * @param recovery recovery publisher
     * @param properties connector settings
     * @param batches pseudonym batch service
     * @param outage outage coordinator
     */
    @Autowired
    public ConnectorListener(JsonMessageMapper mapper, OperationService operations,
                               RecoveryPublisher recovery, ConnectorProperties properties,
                               PseudonymBatchService batches, OutageCoordinator outage) {
        this(mapper, operations, recovery, properties, batches, outage, null);
    }

    /** Creates the listener with authenticated resource preflight.
     * @param mapper source mapper
     * @param operations operation service
     * @param recovery recovery publisher
     * @param properties connector settings
     * @param batches batch service
     * @param outage outage coordinator
     * @param preflight resource preflight
     */
    public ConnectorListener(JsonMessageMapper mapper, OperationService operations,
                             RecoveryPublisher recovery, ConnectorProperties properties,
                             PseudonymBatchService batches, OutageCoordinator outage,
                             TrustDeckPreflight preflight) {
        this.mapper = mapper;
        this.operations = operations;
        this.recovery = recovery;
        this.properties = properties;
        this.batches = batches;
        this.outage = outage;
        this.preflight = preflight;
    }

    /**
     * Processes one source record on its consumer thread.
     *
     * @param records source records
     * @param acknowledgment manual acknowledgment
     * @throws Exception when recovery publication is not confirmed
     */
    @KafkaListener(topics = "${connector.source-topic}", groupId = "${connector.group-id}")
    public void onMessage(List<ConsumerRecord<byte[], byte[]>> records, Acknowledgment acknowledgment) throws Exception {
        List<MappedMessage> mapped = new java.util.ArrayList<>();
        for (int index = 0; index < records.size(); index++) {
            ConsumerRecord<byte[], byte[]> record = records.get(index);
            if (record.value() == null || record.value().length > properties.getMaxRecordBytes()) {
                publishDlt(record, null, "INVALID_RECORD", 0,
                        new MappingException("Kafka value is null or exceeds the configured size."));
            mapped.add(null);
            } else {
                try {
                    mapped.add(mapper.map(record.value()));
                } catch (MappingException exception) {
                    publishDlt(record, null, "INVALID_MAPPING", 0, exception);
                    mapped.add(null);
                }
            }
        }
        if (properties.getOperation().equals("CREATE_PSEUDONYM") && safeBatch(records, mapped)) {
            try {
                verifyTargets(mapped);
                batches.execute(mapped);
            } catch (Exception failure) {
                if (systemic(failure)) {
                    outage.pause(category(failure), failedDomain(mapped), failedEntityType(mapped),
                            properties.getTrustdeck().getProject());
                    throw failure;
                }
                boolean unknown = failure instanceof IllegalStateException;
                for (int index = 0; index < records.size(); index++) {
                    if (unknown) {
                        publishUnknown(records.get(index), mapped.get(index), failure,
                                operations.completedUuid(correlation(records.get(index))));
                    } else {
                        publishDlt(records.get(index), mapped.get(index), category(failure), 0, failure);
                    }
                    acknowledgment.acknowledge(index);
                }
                return;
            }
            acknowledgment.acknowledge();
            return;
        }
        for (int index = 0; index < records.size(); index++) {
            if (mapped.get(index) == null) {
                acknowledgment.acknowledge(index);
                continue;
            }
            processOne(records.get(index), mapped.get(index), acknowledgment, index);
        }
    }

    /**
     * Processes one mapped record while preserving its offset contract.
     *
     * @param record source record
     * @param mapped mapped value
     * @param acknowledgment batch acknowledgment
     * @param index record index
     * @throws Exception when recovery publication is not confirmed
     */
    private void processOne(ConsumerRecord<byte[], byte[]> record, MappedMessage mapped,
                            Acknowledgment acknowledgment, int index) throws Exception {
        try {
            verifyTargets(List.of(mapped));
            operations.execute(mapped, correlation(record));
            // Future result publishing belongs here, with the source correlation and returned object.
            acknowledgment.acknowledge(index);
        } catch (RecordLinkageConflictException exception) {
            publishReview(record, mapped, 1, exception);
            acknowledgment.acknowledge(index);
        } catch (MappingException | IllegalArgumentException exception) {
            publishDlt(record, mapped, "INVALID_MAPPING", 0, exception);
            acknowledgment.acknowledge(index);
        } catch (Exception exception) {
            retryOrDlt(record, mapped, exception, acknowledgment, index);
        }
    }

    /** Performs authenticated checks for dynamic targets before external work. */
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
     * Determines whether one poll can be sent as one safe contiguous batch.
     *
     * @param records source records
     * @param mapped mapped messages
     * @return true when response correlation is deterministic
     */
    private boolean safeBatch(List<ConsumerRecord<byte[], byte[]>> records, List<MappedMessage> mapped) {
        if (records.isEmpty() || mapped.stream().anyMatch(java.util.Objects::isNull)) {
            return false;
        }
        int partition = records.getFirst().partition();
        MappedMessage first = mapped.getFirst();
        String domain = first.domain();
        return records.stream().allMatch(record -> record.partition() == partition)
                && mapped.stream().allMatch(message -> domain.equals(message.domain()) && message.psn() != null
                        && java.util.Objects.equals(first.validityTime(), message.validityTime())
                        && java.util.Objects.equals(first.validFrom(), message.validFrom())
                        && java.util.Objects.equals(first.validTo(), message.validTo()));
    }

    /**
     * Performs bounded record-local retries before publishing a DLT envelope.
     *
     * @param record source record
     * @param mapped mapped target
     * @param failure operation failure
     * @param acknowledgment manual acknowledgment
     * @throws Exception when DLT publication is not confirmed
     */
    private void retryOrDlt(ConsumerRecord<byte[], byte[]> record, MappedMessage mapped,
                            Exception failure, Acknowledgment acknowledgment, int index) throws Exception {
        if (systemic(failure)) {
            outage.pause(category(failure), mapped.domain(), mapped.entityType(),
                    properties.getTrustdeck().getProject());
            throw failure;
        }
        int attempts = properties.getRecovery().getMaxAttempts();
        for (int attempt = 1; attempt <= attempts; attempt++) {
            try {
                Thread.sleep(retryDelay(failure, attempt));
                operations.execute(mapped, correlation(record));
                acknowledgment.acknowledge(index);
                return;
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw exception;
            } catch (Exception retryFailure) {
                failure = retryFailure;
                if (systemic(retryFailure)) {
                    outage.pause(category(retryFailure), mapped.domain(), mapped.entityType(),
                            properties.getTrustdeck().getProject());
                    throw retryFailure;
                }
            }
        }
        publishDlt(record, mapped, category(failure), attempts, failure);
        acknowledgment.acknowledge(index);
    }

    /** Selects a bounded server-directed delay, or the existing exponential fallback. */
    private long retryDelay(Exception failure, int attempt) {
        if (failure instanceof org.trustdeck.client.exception.TrustDeckResponseException response) {
            Long retryAfter = retryAfterMillis(response.getRetryAfter());
            if (retryAfter != null) {
                return retryAfter;
            }
        }
        return Math.min(Duration.ofSeconds(30).toMillis(),
                properties.getRecovery().getInitialBackoff().toMillis() * (1L << Math.min(attempt - 1, 5)));
    }

    /** Parses Retry-After delta-seconds or an HTTP date and clamps it to the retry safety bound. */
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
                long millis = Duration.between(Instant.now(),
                        ZonedDateTime.parse(header, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant()).toMillis();
                return Math.max(0, Math.min(Duration.ofSeconds(30).toMillis(), millis));
            } catch (RuntimeException invalidHeader) {
                return null;
            }
        }
    }

    /** Identifies backend/authentication failures that must not be drained to recovery topics. */
    private boolean systemic(Exception failure) {
        if (failure instanceof org.trustdeck.client.exception.TrustDeckResponseException response) {
            return response.getStatusCode() == 401 || response.getStatusCode() == 403
                    || response.getStatusCode() >= 500;
        }
        return failure instanceof org.apache.kafka.common.KafkaException
                || failure instanceof java.util.concurrent.TimeoutException
                || failure instanceof java.util.concurrent.ExecutionException
                || failure instanceof java.net.ConnectException
                || failure instanceof java.net.SocketTimeoutException
                || failure instanceof java.io.IOException
                || failure.getCause() instanceof java.io.IOException
                || failure.getCause() instanceof org.apache.kafka.common.KafkaException
                || failure.getCause() instanceof java.util.concurrent.TimeoutException;
    }

    /** Converts known failures to a safe recovery category. */
    private String category(Exception failure) {
        if (failure instanceof org.trustdeck.client.exception.TrustDeckResponseException response) {
            return response.getStatusCode() == 401 || response.getStatusCode() == 403
                    ? "AUTHORIZATION_FAILURE" : response.getStatusCode() >= 500
                    ? "BACKEND_UNAVAILABLE" : "TRANSIENT_OPERATION_FAILURE";
        }
        if (failure instanceof org.apache.kafka.common.KafkaException
                || failure instanceof java.util.concurrent.TimeoutException
                || failure instanceof java.util.concurrent.ExecutionException
                || failure.getCause() instanceof org.apache.kafka.common.KafkaException
                || failure.getCause() instanceof java.util.concurrent.TimeoutException) {
            return "RECOVERY_PRODUCER_UNAVAILABLE";
        }
        return systemic(failure) ? "BACKEND_UNAVAILABLE" : "TRANSIENT_OPERATION_FAILURE";
    }

    /** Publishes recovery data without acknowledging when the recovery producer is unavailable. */
    private void publishDlt(ConsumerRecord<byte[], byte[]> record, MappedMessage mapped,
                            String category, int attempts, Throwable failure) throws Exception {
        try {
            recovery.publishDlt(record, mapped, category, attempts, failure);
        } catch (Exception publicationFailure) {
            outage.pause("RECOVERY_PRODUCER_UNAVAILABLE");
            throw publicationFailure;
        }
    }

    /** Publishes linkage review data without acknowledging when the recovery producer is unavailable. */
    private void publishReview(ConsumerRecord<byte[], byte[]> record, MappedMessage mapped,
                               int attempts, Throwable failure) throws Exception {
        try {
            recovery.publishReview(record, mapped, attempts, failure);
        } catch (Exception publicationFailure) {
            outage.pause("RECOVERY_PRODUCER_UNAVAILABLE");
            throw publicationFailure;
        }
    }

    /** Publishes an unknown outcome without acknowledging when the recovery producer is unavailable. */
    private void publishUnknown(ConsumerRecord<byte[], byte[]> record, MappedMessage mapped,
                                Throwable failure, java.util.UUID completedUuid) throws Exception {
        try {
            recovery.publishUnknown(record, mapped, failure, completedUuid);
        } catch (Exception publicationFailure) {
            outage.pause("RECOVERY_PRODUCER_UNAVAILABLE");
            throw publicationFailure;
        }
    }

    /** Returns the first dynamic domain involved in a failed batch. */
    private String failedDomain(List<MappedMessage> mapped) {
        return mapped.isEmpty() || mapped.getFirst() == null ? null : mapped.getFirst().domain();
    }

    /** Returns the first dynamic entity type involved in a failed batch. */
    private String failedEntityType(List<MappedMessage> mapped) {
        return mapped.isEmpty() || mapped.getFirst() == null ? null : mapped.getFirst().entityType();
    }

    /**
     * Builds a deterministic source correlation for bounded combined-stage replay.
     *
     * @param record source record
     * @return source correlation
     */
    private String correlation(ConsumerRecord<byte[], byte[]> record) {
        return record.topic() + "/" + record.partition() + "/" + record.offset();
    }
}
