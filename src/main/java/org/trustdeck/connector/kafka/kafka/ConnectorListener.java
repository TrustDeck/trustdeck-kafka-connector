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
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
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

    /**
     * Creates the listener.
     *
     * @param mapper source mapper
     * @param operations operation service
     * @param recovery recovery publisher
     * @param properties connector settings
     * @param batches pseudonym batch service
     */
    public ConnectorListener(JsonMessageMapper mapper, OperationService operations,
                             RecoveryPublisher recovery, ConnectorProperties properties,
                             PseudonymBatchService batches) {
        this.mapper = mapper;
        this.operations = operations;
        this.recovery = recovery;
        this.properties = properties;
        this.batches = batches;
    }

    /**
     * Processes one source record on its consumer thread.
     *
     * @param records source records
     * @param acknowledgment manual acknowledgment
     * @throws Exception when recovery publication is not confirmed
     */
    @KafkaListener(topics = "${connector.source-topic}", groupId = "${connector.group-id}")
    public void onMessage(List<ConsumerRecord<String, byte[]>> records, Acknowledgment acknowledgment) throws Exception {
        List<MappedMessage> mapped = new java.util.ArrayList<>();
        for (int index = 0; index < records.size(); index++) {
            ConsumerRecord<String, byte[]> record = records.get(index);
            if (record.value() == null || record.value().length > properties.getMaxRecordBytes()) {
                recovery.publishDlt(record, null, "INVALID_RECORD", 0,
                        new MappingException("Kafka value is null or exceeds the configured size."));
                acknowledgment.acknowledge(index);
                mapped.add(null);
            } else {
                try {
                    mapped.add(mapper.map(record.value()));
                } catch (MappingException exception) {
                    recovery.publishDlt(record, null, "INVALID_MAPPING", 0, exception);
                    acknowledgment.acknowledge(index);
                    mapped.add(null);
                }
            }
        }
        if (properties.getOperation().equals("CREATE_PSEUDONYM") && safeBatch(records, mapped)) {
            batches.execute(mapped);
            acknowledgment.acknowledge();
            return;
        }
        for (int index = 0; index < records.size(); index++) {
            if (mapped.get(index) == null) {
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
    private void processOne(ConsumerRecord<String, byte[]> record, MappedMessage mapped,
                            Acknowledgment acknowledgment, int index) throws Exception {
        try {
            operations.execute(mapped, correlation(record));
            // Future result publishing belongs here, with the source correlation and returned object.
            acknowledgment.acknowledge(index);
        } catch (RecordLinkageConflictException exception) {
            recovery.publishReview(record, mapped, 1, exception);
            acknowledgment.acknowledge(index);
        } catch (MappingException | IllegalArgumentException exception) {
            recovery.publishDlt(record, mapped, "INVALID_MAPPING", 0, exception);
            acknowledgment.acknowledge(index);
        } catch (Exception exception) {
            retryOrDlt(record, mapped, exception, acknowledgment, index);
        }
    }

    /**
     * Determines whether one poll can be sent as one safe contiguous batch.
     *
     * @param records source records
     * @param mapped mapped messages
     * @return true when response correlation is deterministic
     */
    private boolean safeBatch(List<ConsumerRecord<String, byte[]>> records, List<MappedMessage> mapped) {
        if (records.isEmpty() || mapped.stream().anyMatch(java.util.Objects::isNull)) {
            return false;
        }
        int partition = records.getFirst().partition();
        String domain = mapped.getFirst().domain();
        return records.stream().allMatch(record -> record.partition() == partition)
                && mapped.stream().allMatch(message -> domain.equals(message.domain()) && message.psn() != null);
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
    private void retryOrDlt(ConsumerRecord<String, byte[]> record, MappedMessage mapped,
                            Exception failure, Acknowledgment acknowledgment, int index) throws Exception {
        int attempts = properties.getRecovery().getMaxAttempts();
        for (int attempt = 1; attempt <= attempts; attempt++) {
            try {
                Thread.sleep(Math.min(Duration.ofSeconds(30).toMillis(),
                        properties.getRecovery().getInitialBackoff().toMillis() * (1L << Math.min(attempt - 1, 5))));
                operations.execute(mapped, correlation(record));
                acknowledgment.acknowledge(index);
                return;
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw exception;
            } catch (Exception ignored) {
                // Preserve the original failure category if all bounded attempts fail.
            }
        }
        recovery.publishDlt(record, mapped, "TRANSIENT_OPERATION_FAILURE", attempts, failure);
        acknowledgment.acknowledge(index);
    }

    /**
     * Builds a deterministic source correlation for bounded combined-stage replay.
     *
     * @param record source record
     * @return source correlation
     */
    private String correlation(ConsumerRecord<String, byte[]> record) {
        return record.topic() + "/" + record.partition() + "/" + record.offset();
    }
}
