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
import java.util.Base64;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.trustdeck.connector.kafka.config.ConnectorProperties;
import org.trustdeck.connector.kafka.mapping.MappedMessage;

/** Publishes confirmed DLT and linkage-review envelopes. */
@Service
public class RecoveryPublisher {

    /** JSON serializer. */
    private final ObjectMapper mapper;

    /** Recovery producer. */
    private final KafkaTemplate<String, String> template;

    /** Connector settings. */
    private final ConnectorProperties properties;

    /**
     * Creates the publisher.
     *
     * @param template Kafka producer
     * @param properties connector settings
     * @param mapper Spring-configured JSON mapper
     */
    public RecoveryPublisher(KafkaTemplate<String, String> template, ConnectorProperties properties,
                             ObjectMapper mapper) {
        this.template = template;
        this.properties = properties;
        this.mapper = mapper;
    }

    /**
     * Publishes a DLT envelope and waits for broker confirmation.
     *
     * @param record source record
     * @param mapped mapped target, possibly null
     * @param category safe failure category
     * @param attempts delivery attempts
     * @param failure failure exception
     * @throws Exception when broker confirmation fails
     */
    public void publishDlt(ConsumerRecord<byte[], byte[]> record, MappedMessage mapped,
                          String category, int attempts, Throwable failure) throws Exception {
        publish(record, mapped, category, "FAILED_DLT", attempts, failure,
                properties.getRecovery().getDltTopic());
    }

    /**
     * Publishes a linkage review envelope and waits for broker confirmation.
     *
     * @param record source record
     * @param mapped mapped target
     * @param attempts delivery attempts
     * @param failure linkage failure
     * @throws Exception when broker confirmation fails
     */
    public void publishReview(ConsumerRecord<byte[], byte[]> record, MappedMessage mapped,
                              int attempts, Throwable failure) throws Exception {
        publish(record, mapped, "RECORD_LINKAGE", "PENDING_LINKAGE_REVIEW", attempts, failure,
                properties.getRecovery().getReviewTopic());
    }

    /**
     * Serializes and confirms one envelope.
     *
     * @param record source record
     * @param mapped mapped target
     * @param category failure category
     * @param status envelope status
     * @param attempts attempts
     * @param failure failure
     * @param topic destination topic
     * @throws Exception when serialization or publication fails
     */
    private void publish(ConsumerRecord<byte[], byte[]> record, MappedMessage mapped,
                         String category, String status, int attempts, Throwable failure,
                         String topic) throws Exception {
        RecoveryEnvelope envelope = new RecoveryEnvelope("1", status, record.topic(), record.partition(),
                record.offset(), record.timestamp() < 0 ? null : java.time.Instant.ofEpochMilli(record.timestamp()),
                properties.getOperation(), mapped == null ? null : mapped.domain(), category, null, attempts,
                null, null, record.key() == null ? null : Base64.getEncoder().encodeToString(record.key()),
                Base64.getEncoder().encodeToString(record.value()), mapped == null ? null : mapped.username());
        String json = mapper.writeValueAsString(envelope);
        String key = record.key() == null ? null : Base64.getEncoder().encodeToString(record.key());
        template.send(topic, key, json).get(Duration.ofSeconds(10).toMillis(), TimeUnit.MILLISECONDS);
    }
}
