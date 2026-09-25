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

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
	 * @return configured source consumer factory
	 */
    @Bean
    public ConsumerFactory<byte[], byte[]> consumerFactory(KafkaProperties kafkaProperties, Environment environment) {
        Map<String, Object> settings = kafkaProperties.buildConsumerProperties();
        settings.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        
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
	 * @param consumerFactory source consumer factory
	 * @param properties connector settings controlling consumer concurrency
	 * @param autoStartup whether listeners start automatically with the application
	 * @return configured Kafka listener container factory
	 */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<byte[], byte[]> kafkaListenerContainerFactory(
            ConsumerFactory<byte[], byte[]> consumerFactory, ConnectorProperties properties,
            @Value("${spring.kafka.listener.auto-startup:true}") boolean autoStartup) {
    	
        ConcurrentKafkaListenerContainerFactory<byte[], byte[]> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(properties.getConcurrency());
        factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setMissingTopicsFatal(true);
        factory.setBatchListener(true);
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
     * Escapes values embedded in a Kafka JAAS quoted string.
     *
     * @param value raw setting
     * @return escaped setting
     */
    private String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
