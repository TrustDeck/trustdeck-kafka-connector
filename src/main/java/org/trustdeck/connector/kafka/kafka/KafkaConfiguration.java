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

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.trustdeck.connector.kafka.config.ConnectorProperties;

/** Configures bounded concurrent Kafka consumers and confirmed recovery producers. */
@Configuration
public class KafkaConfiguration {

    /** Creates Kafka bean configuration. */
    public KafkaConfiguration() {
    }

    /**
     * Creates a byte-preserving source consumer factory.
     *
     * @param kafkaProperties Spring Kafka settings
     * @param environment environment values
     * @return consumer factory
     */
    @Bean
    public ConsumerFactory<byte[], byte[]> consumerFactory(KafkaProperties kafkaProperties, Environment environment) {
        Map<String, Object> settings = kafkaProperties.buildConsumerProperties();
        settings.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        addJaasCredentials(settings, environment);
        return new DefaultKafkaConsumerFactory<>(settings, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    /**
     * Creates the listener container factory.
     *
     * @param consumerFactory source consumer factory
     * @param properties connector settings
     * @return listener factory
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<byte[], byte[]> kafkaListenerContainerFactory(
            ConsumerFactory<byte[], byte[]> consumerFactory, ConnectorProperties properties) {
        ConcurrentKafkaListenerContainerFactory<byte[], byte[]> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(properties.getConcurrency());
        factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setMissingTopicsFatal(true);
        factory.setBatchListener(true);
        return factory;
    }

    /**
     * Creates the byte-preserving recovery producer.
     *
     * @param kafkaProperties Spring Kafka settings
     * @param environment environment values
     * @return producer factory
     */
    @Bean
    public ProducerFactory<String, String> producerFactory(KafkaProperties kafkaProperties, Environment environment) {
        Map<String, Object> settings = kafkaProperties.buildProducerProperties();
        addJaasCredentials(settings, environment);
        return new DefaultKafkaProducerFactory<>(settings, new StringSerializer(), new StringSerializer());
    }

    /**
     * Creates the recovery Kafka template.
     *
     * @param producerFactory producer factory
     * @return Kafka template
     */
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    /**
     * Adds escaped familiar username/password settings when a complete JAAS string was not supplied.
     *
     * @param settings Kafka client settings
     * @param environment environment values
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
