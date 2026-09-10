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

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/** Validated connector-wide settings and bounded processing limits. */
@Data
@Validated
@ConfigurationProperties(prefix = "connector")
public class ConnectorProperties {

    /** Creates empty settings for Spring property binding. */
    public ConnectorProperties() {
    }

    /** Selected operation processed by this application instance. */
    @NotBlank
    private String operation;

    /** Source Kafka topic. */
    @NotBlank
    private String sourceTopic;

    /** Kafka consumer group. */
    @NotBlank
    private String groupId;

    /** Maximum accepted Kafka value bytes. */
    @Min(1)
    private int maxRecordBytes = 1_048_576;

    /** Maximum source records returned from one poll. */
    @Min(1)
    private int maxPollRecords = 100;

    /** Number of listener containers. */
    @Min(1)
    private int concurrency = 2;

    /** Maximum records sent in one pseudonym create batch. */
    @Min(1)
    private int batchSize = 100;

    /** Maximum bytes sent in one pseudonym create batch. */
    @Min(1)
    private int batchBytes = 262_144;

    /** HTTP connection timeout. */
    @NotNull
    private Duration connectTimeout = Duration.ofSeconds(5);

    /** HTTP response read timeout. */
    @NotNull
    private Duration readTimeout = Duration.ofSeconds(30);

    /** TrustDeck and token endpoint settings. */
    @Valid
    private TrustDeckProperties trustdeck = new TrustDeckProperties();

    /** Source-to-TrustDeck mapping settings. */
    @Valid
    private MappingProperties mapping = new MappingProperties();

    /** Recovery topic and retry settings. */
    @Valid
    private RecoveryProperties recovery = new RecoveryProperties();

    /** TrustDeck connection and fixed resource target settings. */
    @Data
    public static class TrustDeckProperties {
        /** Creates empty TrustDeck settings for property binding. */
        public TrustDeckProperties() {
        }
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

    /** Explicit JSON Pointer mappings and timestamp rules. */
    @Data
    public static class MappingProperties {
        /** Creates empty mapping settings for property binding. */
        public MappingProperties() {
        }
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

        /** Optional target entity data mapping definitions. */
        private java.util.List<FieldMapping> entityFields = new java.util.ArrayList<>();

        /** IANA zone for offset-free source timestamps. */
        @NotBlank
        private String sourceTimezone = "UTC";

        /** Strict format for offset-free timestamps. */
        private String datetimeFormat;
    }

    /** One explicit source-to-target mapping for entity data or identifiers. */
    @Data
    public static class FieldMapping {
        /** Creates an empty field mapping for property binding. */
        public FieldMapping() {
        }
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

    /** DLT/review topics and bounded recovery behavior. */
    @Data
    public static class RecoveryProperties {
        /** Creates empty recovery settings for property binding. */
        public RecoveryProperties() {
        }
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
}
