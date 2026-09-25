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
import java.util.ArrayList;
import java.util.List;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Configuration properties controlling Kafka consumption, TrustDeck access,
 * message mappings, recovery handling, and statistics collection.
 *
 * Instances are populated through Spring property binding and validated before
 * the connector starts processing records.
 * 
 * @author Armin Müller
 */
@Data
@Validated
@ConfigurationProperties(prefix = "connector")
public class ConnectorProperties {

    /** Creates empty settings for Spring property binding. */
    public ConnectorProperties() { }

    /** Selected operation processed by this application instance. */
    @NotBlank
    private String operation;

    /** Source Kafka topic. */
    @NotBlank
    private String sourceTopic;

    /** Kafka consumer group. */
    @NotBlank
    private String groupId;

    /** Maximum accepted Kafka value bytes for one record. */
    @Min(1)
    private int maxRecordBytes = 1_048_576;

    /** Maximum source records returned from one poll. */
    @Min(1)
    private int maxPollRecords = 100;

    /** Number of concurrent Kafka consumers used to process source records. */
    @Min(1)
    private int concurrency = 2;

    /** Maximum records sent in one pseudonym create batch. */
    @Min(1)
    private int batchSize = 10_000;

    /** Maximum bytes sent in one pseudonym create batch. */
    @Min(1)
    private int batchBytes = 1_048_576;

    /** HTTP connection timeout. */
    @NotNull
    private Duration connectTimeout = Duration.ofSeconds(5);

    /** HTTP response read timeout. */
    @NotNull
    private Duration readTimeout = Duration.ofSeconds(60);

    /** TrustDeck and token endpoint settings. */
    @Valid
    private TrustDeckProperties trustdeck = new TrustDeckProperties();

    /** Source-to-TrustDeck mapping settings. */
    @Valid
    private MappingProperties mapping = new MappingProperties();

    /** Recovery topic and retry settings. */
    @Valid
    private RecoveryProperties recovery = new RecoveryProperties();

    /** Optional asynchronous runtime and backend statistics collection. */
    @Valid
    private StatisticsProperties statistics = new StatisticsProperties();

    /**
     * TrustDeck connection settings and optional fixed resource targets.
     *
     * Contains the service and token endpoints, OAuth client credentials, and
     * optional project and entity type values used by operations that do not
     * resolve these targets from incoming records.
     */
    @Data
    public static class TrustDeckProperties {
        
    	/** Creates empty TrustDeck settings for property binding. */
        public TrustDeckProperties() { }
        
        /** TrustDeck service base URL. */
        @NotBlank
        private String serviceUrl;

        /** OAuth token endpoint. */
        @NotBlank
        private String tokenEndpoint;

        /** OAuth confidential client ID. */
        @NotBlank
        private String clientId;

        /** OAuth confidential client secret. */
        @NotBlank
        private String clientSecret;

        /** Fixed project for entity operations. */
        private String project;

        /** Fixed entity type fallback. */
        private String entityType;
    }

    /**
     * Groups all source-to-TrustDeck mapping configurations.
     *
     * Common mappings are shared between operations, while update-specific mappings
     * define how existing pseudonym values are located and replaced.
     */
    @Data
    public static class MappingProperties {
        
    	/** Creates empty mapping settings for property binding. */
        public MappingProperties() { }
        
        /** Mappings shared by the selected operation and entity operations. */
        @Valid
        private CommonMappingProperties common = new CommonMappingProperties();

        /** Selectors and replacement fields used only by pseudonym updates. */
        @Valid
        private UpdatePseudonymMappingProperties updatePseudonym = new UpdatePseudonymMappingProperties();
    }

    /**
     * Mapping settings shared by pseudonym and entity operations.
     *
     * Defines source paths, optional constant values, entity field mappings,
     * timestamp conversion settings, and optional record-specific TrustDeck targets.
     */
    @Data
    public static class CommonMappingProperties {
        
    	/** Creates common mappings with UTC as the default source timezone. */
        public CommonMappingProperties() { }
        
        /** Constant domain override. */
        private String domainConstant;
        
        /** Message pointer for a domain. */
        private String domainPath;
        
        /** Message pointer for the pseudonym identifier. */
        private String identifierPath;
        
        /** Message pointer for the identifier type. */
        private String idTypePath;
        
        /** Message pointer for an imported pseudonym value. */
        private String psnPath;
        
        /** Optional message pointer for a pseudonym validity duration. */
        private String validityTimePath;
        
        /** Message pointer for valid-from conversion. */
        private String validFromPath;
        
        /** Message pointer for valid-to conversion. */
        private String validToPath;
        
        /** Optional message pointer for source provenance. */
        private String usernamePath;
        
        /** Optional message pointer for a project entity type. */
        private String entityTypePath;
        
        /** Optional message pointer for a TrustDeck entity UUID. */
        private String entityIdPath;
        
        /** Entity data mappings shared by entity create, update, and combined operations. */
        private List<FieldMapping> entityFields = new ArrayList<>();
        
        /** IANA zone for offset-free source timestamps. */
        @NotBlank
        private String sourceTimezone = "UTC";
        
        /** Strict format for offset-free timestamps. */
        private String datetimeFormat;
    }

    /**
     * Mapping settings specific to pseudonym update operations.
     *
     * Defines how the existing pseudonym is identified and which incoming values
     * should replace its identifier, type, value, or domain.
     */
    @Data
    public static class UpdatePseudonymMappingProperties {
    	
        /** Creates empty pseudonym update mappings for property binding. */
        public UpdatePseudonymMappingProperties() { }
        
        /** Message pointer for the old pseudonym identifier. */
        private String oldIdentifierPath;
        
        /** Message pointer for the old pseudonym type. */
        private String oldIdTypePath;
        
        /** Message pointer for the old pseudonym value. */
        private String oldPsnPath;
        
        /** Message pointer for a replacement pseudonym identifier. */
        private String newIdentifierPath;
        
        /** Message pointer for a replacement pseudonym type. */
        private String newIdTypePath;
        
        /** Message pointer for a replacement pseudonym value. */
        private String newPsnPath;
        
        /** Message pointer for the old pseudonym domain. */
        private String oldDomainPath;
        
        /** Message pointer for a replacement pseudonym domain. */
        private String newDomainPath;
    }

    /**
     * Defines one source-to-target mapping for entity data.
     *
     * A mapping reads a value from the incoming record or uses a configured constant
     * and writes the resulting value to the configured target JSON Pointer.
     */
    @Data
    public static class FieldMapping {
        
    	/** Creates an empty field mapping for property binding. */
        public FieldMapping() { }
        
        /** Source JSON Pointer or bare top-level field. */
        @NotBlank
        private String sourcePath;

        /** Target JSON Pointer within entity data. */
        @NotBlank
        private String targetPath;

        /** Whether the mapped source must be present and non-null. */
        private boolean required;

        /** Optional constant value taking precedence over the source. */
        private String constant;
    }

    /**
     * Configures dead-letter, review, retry, and backend availability handling.
     *
     * Defines the recovery topics and the bounded retry behavior used when records
     * cannot be processed successfully on the first attempt.
     */
    @Data
    public static class RecoveryProperties {
        
    	/** Creates empty recovery settings for property binding. */
        public RecoveryProperties() { }
        
        /** Dead-letter topic. */
        private String dltTopic;

        /** Linkage-review topic. */
        private String reviewTopic;

        /** Maximum transient attempts. */
        @Min(0)
        private int maxAttempts = 3;

        /** Initial retry delay. */
        private Duration initialBackoff = Duration.ofSeconds(1);

        /** Availability probe interval. */
        private Duration probeInterval = Duration.ofSeconds(15);
    }

    /**
     * Configures optional runtime and TrustDeck backend statistics collection.
     *
     * Controls whether statistics are collected, how frequently backend statistics
     * are requested, and where the completed statistics file is written.
     */
    @Data
    public static class StatisticsProperties {
        
    	/** Creates disabled statistics settings for property binding. */
        public StatisticsProperties() { }

        /** Whether statistics should be collected during a connector-run. */
        private boolean enabled;

        /** Delay between backend statistics requests. */
        @NotNull
        private Duration interval = Duration.ofSeconds(10);

        /** Directory in which the completed run file is written. */
        @NotBlank
        private String outputDirectory = "statistics";
    }
}
