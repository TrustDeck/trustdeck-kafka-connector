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

package org.trustdeck.connector.kafka.config;

import java.time.Duration;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.trustdeck.connector.kafka.health.OutageCoordinator;
import org.trustdeck.connector.kafka.listener.ListenerErrorHandler;

/**
 * Configures the Kafka consumer, listener container, producer, and template used
 * by the connector.
 *
 * Source records are consumed as raw byte arrays to preserve their original
 * representation. Recovery messages are produced as strings. Kafka client
 * authentication can either use a complete JAAS configuration or credentials
 * supplied through environment properties.
 * 
 * @author Armin Müller
 */
@Configuration
public class KafkaConfiguration {

    /** Creates Kafka bean configuration. */
    public KafkaConfiguration() { }

    /**
	 * Creates the consumer factory used for source records.
	 *
	 * Values and keys are deserialized as raw byte arrays so that the original
	 * Kafka record content is preserved. Automatic offset commits are disabled
	 * because offsets are acknowledged explicitly by the listener.
	 *
	 * @param kafkaProperties Spring Kafka consumer settings
	 * @param environment environment values used for optional JAAS credentials
	 * @param properties connector settings used to validate the poll interval
	 * @return configured source consumer factory
	 */
    @Bean
    public ConsumerFactory<byte[], byte[]> consumerFactory(KafkaProperties kafkaProperties, Environment environment,
    				ConnectorProperties properties) {
        Map<String, Object> settings = kafkaProperties.buildConsumerProperties();
        settings.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        validatePollInterval(settings, properties);
        addJaasCredentials(settings, environment);
        
        return new DefaultKafkaConsumerFactory<>(settings, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    /**
	 * Creates the Kafka listener container factory used for source processing.
	 *
	 * The factory uses the configured number of concurrent consumers, batch
	 * delivery, immediate manual acknowledgements, and fails startup when the
	 * configured source topic does not exist.
	 *
	 * The listener handles systemic failures itself through {@code nack}. Any
	 * other exception that escapes it is handled by {@link ListenerErrorHandler},
	 * which never retries or discards a batch, unlike Spring's default handler.
	 *
	 * @param consumerFactory source consumer factory
	 * @param properties connector settings controlling consumer concurrency
	 * @param autoStartup whether listeners start automatically with the application
	 * @param outage lazily resolved outage coordinator used by the error handler
	 * @return configured Kafka listener container factory
	 */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<byte[], byte[]> kafkaListenerContainerFactory(
            ConsumerFactory<byte[], byte[]> consumerFactory, ConnectorProperties properties,
            @Value("${spring.kafka.listener.auto-startup:true}") boolean autoStartup,
            ObjectProvider<OutageCoordinator> outage) {

        ConcurrentKafkaListenerContainerFactory<byte[], byte[]> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(properties.getConcurrency());
        factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setMissingTopicsFatal(true);
        factory.setBatchListener(true);
        factory.setCommonErrorHandler(new ListenerErrorHandler(outage));
        factory.setAutoStartup(autoStartup);
        
        return factory;
    }

    /**
	 * Creates the producer factory used for recovery messages.
	 *
	 * Producer settings are inherited from Spring Kafka and extended with JAAS
	 * credentials from the environment when no complete JAAS configuration was
	 * supplied.
	 *
	 * @param kafkaProperties Spring Kafka producer settings
	 * @param environment environment values used for optional JAAS credentials
	 * @return configured recovery producer factory
	 */
    @Bean
    public ProducerFactory<String, String> producerFactory(KafkaProperties kafkaProperties, Environment environment) {
        Map<String, Object> settings = kafkaProperties.buildProducerProperties();
        
        addJaasCredentials(settings, environment);
        
        return new DefaultKafkaProducerFactory<>(settings, new StringSerializer(), new StringSerializer());
    }

    /**
	 * Creates the Kafka template used to publish recovery messages.
	 *
	 * @param producerFactory producer factory used by the template
	 * @return configured recovery Kafka template
	 */
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    /**
	 * Adds a JAAS configuration from separate username and password properties
	 * when no complete JAAS configuration was supplied.
	 *
	 * Credentials are escaped before being embedded in the quoted JAAS string.
	 * Existing explicit JAAS configuration always takes precedence.
	 *
	 * @param settings Kafka client settings to update
	 * @param environment environment containing Kafka authentication properties
	 */
    private void addJaasCredentials(Map<String, Object> settings, Environment environment) {
        String configured = environment.getProperty("KAFKA_SASL_JAAS_CONFIG", "");
        String username = environment.getProperty("KAFKA_USERNAME", "");
        String password = environment.getProperty("KAFKA_PASSWORD", "");
        
        if (configured.isBlank() && !username.isBlank() && !password.isBlank()) {
            settings.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required "
                    + "username=\"" + escape(username) + "\" password=\"" + escape(password) + "\";");
        }
    }

    /**
	 * Verifies that {@code max.poll.interval.ms} covers the batch time budget plus
	 * one in-flight attempt.
	 *
	 * After the budget expires, the listener starts no new record or retry, but the
	 * attempt in progress still has to finish. One attempt is assumed to need at
	 * most two sequential HTTP calls (token refresh and operation), each bounded by
	 * the connect and read timeouts. If the poll interval is shorter, a slow batch
	 * makes the consumer miss its poll deadline and triggers a rebalance.
	 *
	 * @param settings consumer settings
	 * @param properties connector settings
	 * @throws IllegalArgumentException if the poll interval is too short
	 */
    static void validatePollInterval(Map<String, Object> settings, ConnectorProperties properties) {
        Object configured = settings.get(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG);
        Duration pollInterval = Duration.ofMillis(configured == null ? 300_000L : Long.parseLong(configured.toString().trim()));
        Duration inFlight = properties.getConnectTimeout().plus(properties.getReadTimeout()).multipliedBy(2);
        Duration required = properties.getRecovery().getBatchTimeBudget().plus(inFlight);

        if (pollInterval.compareTo(required) < 0) {
            throw new IllegalArgumentException("max.poll.interval.ms (" + pollInterval.toMillis()
                    + ") must be at least recovery.batch-time-budget + 2 x (connect-timeout + read-timeout) = "
                    + required.toMillis() + " ms.");
        }
    }

    /**
     * Escapes values embedded in a Kafka JAAS quoted string.
     *
     * @param value raw setting
     * @return escaped setting
     */
    private String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
