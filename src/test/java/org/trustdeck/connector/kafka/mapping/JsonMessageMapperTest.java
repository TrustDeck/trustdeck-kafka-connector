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

package org.trustdeck.connector.kafka.mapping;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.LocalDateTime;

import org.junit.jupiter.api.Test;
import org.trustdeck.connector.kafka.config.ConnectorProperties;

/** Verifies strict source mapping, precedence, JSON types, and Berlin time conversion. */
class JsonMessageMapperTest {

    /**
     * Maps the approved pseudonym sample without losing a leading zero.
     */
    @Test
    void mapsSampleWithBerlinTimezone() {
        ConnectorProperties properties = properties();
        JsonMessageMapper mapper = new JsonMessageMapper(properties);

        MappedMessage mapped = mapper.map("{\"value\":\"0123456789\",\"type\":\"case_number\",\"domain\":\"datalake\",\"time\":\"2025-10-09 21:08:10.175\",\"validity\":\"1d\"}".getBytes());

        assertEquals("0123456789", mapped.identifier());
        assertEquals("datalake", mapped.domain());
        assertEquals(LocalDateTime.of(2025, 10, 9, 19, 8, 10, 175_000_000), mapped.validFrom());
        assertEquals("1d", mapped.validityTime());
    }

    /**
     * Confirms a nonblank domain constant wins over the source value.
     */
    @Test
    void constantDomainWins() {
        ConnectorProperties properties = properties();
        properties.getMapping().getCommon().setDomainConstant("fixed/domain");

        assertEquals("fixed/domain", new JsonMessageMapper(properties)
                .map("{\"value\":\"1\",\"type\":\"x\",\"domain\":\"other\"}".getBytes()).domain());
    }

    /**
     * Rejects a DST gap instead of silently selecting an offset.
     */
    @Test
    void rejectsBerlinDstGap() {
        ConnectorProperties properties = properties();
        assertThrows(MappingException.class, () -> new JsonMessageMapper(properties)
                .map("{\"value\":\"1\",\"type\":\"x\",\"domain\":\"d\",\"time\":\"2025-03-30 02:30:00.000\"}".getBytes()));
    }

    /**
     * Rejects a root array because one Kafka record is one object.
     */
    @Test
    void rejectsRootArray() {
        ConnectorProperties properties = properties();
        assertThrows(MappingException.class, () -> new JsonMessageMapper(properties).map("[]".getBytes()));
    }

    /** Rejects malformed UTF-8 before JSON parsing can replace bytes. */
    @Test
    void rejectsMalformedUtf8() {
        ConnectorProperties properties = properties();
        assertThrows(MappingException.class, () -> new JsonMessageMapper(properties)
                .map(new byte[] {(byte) 0xc3, (byte) 0x28}));
    }

    /** Rejects trailing JSON values instead of silently accepting the first object. */
    @Test
    void rejectsTrailingJson() {
        ConnectorProperties properties = properties();
        assertThrows(MappingException.class, () -> new JsonMessageMapper(properties)
                .map("{\"value\":\"1\",\"type\":\"x\",\"domain\":\"d\"} {}".getBytes()));
    }

    /** Rejects a primitive root because Kafka records must contain one JSON object. */
    @Test
    void rejectsPrimitiveRoot() {
        assertThrows(MappingException.class, () -> new JsonMessageMapper(properties()).map("true".getBytes()));
    }

    /** Rejects an ambiguous Berlin overlap. */
    @Test
    void rejectsBerlinDstOverlap() {
        ConnectorProperties properties = properties();
        assertThrows(MappingException.class, () -> new JsonMessageMapper(properties)
                .map("{\"value\":\"1\",\"type\":\"x\",\"domain\":\"d\",\"time\":\"2025-10-26 02:30:00.000\"}".getBytes()));
    }

    /**
     * Creates the approved synthetic pseudonym mapping.
     *
     * @return test properties
     */
    private ConnectorProperties properties() {
        ConnectorProperties properties = new ConnectorProperties();
        properties.setOperation("CREATE_PSEUDONYM");
        properties.setSourceTopic("source");
        properties.setGroupId("group");
        properties.getMapping().getCommon().setDomainPath("/domain");
        properties.getMapping().getCommon().setIdentifierPath("/value");
        properties.getMapping().getCommon().setIdTypePath("/type");
        properties.getMapping().getCommon().setValidityTimePath("/validity");
        properties.getMapping().getCommon().setValidFromPath("/time");
        properties.getMapping().getCommon().setSourceTimezone("Europe/Berlin");
        properties.getMapping().getCommon().setDatetimeFormat("uuuu-MM-dd HH:mm:ss.SSS");
        return properties;
    }
}
