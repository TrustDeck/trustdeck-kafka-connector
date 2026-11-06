/*
 * TrustDeck Kafka Connector
 * Copyright 2026 Armin Müller
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.trustdeck.connector.kafka.logging;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.LoggingEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.ansi.AnsiOutput;

/** Verifies the TrustDeck-style level colors and that coloring can be switched off. */
class LevelColorConverterTest {

    private final LevelColorConverter converter = new LevelColorConverter();

    @AfterEach
    void resetAnsi() {
        AnsiOutput.setEnabled(AnsiOutput.Enabled.DETECT);
    }

    /** Errors red, warnings yellow, information blue, debug uncolored. */
    @Test
    void colorsLevelsLikeTrustDeck() {
        AnsiOutput.setEnabled(AnsiOutput.Enabled.ALWAYS);

        assertEquals("\u001B[31mERROR\u001B[0;39m", converter.transform(event(Level.ERROR), "ERROR"));
        assertEquals("\u001B[33mWARN\u001B[0;39m", converter.transform(event(Level.WARN), "WARN"));
        assertEquals("\u001B[34mINFO\u001B[0;39m", converter.transform(event(Level.INFO), "INFO"));
        assertEquals("DEBUG", converter.transform(event(Level.DEBUG), "DEBUG"));
    }

    /** With spring.output.ansi.enabled=never the text stays plain. */
    @Test
    void leavesTextPlainWhenAnsiIsDisabled() {
        AnsiOutput.setEnabled(AnsiOutput.Enabled.NEVER);

        assertEquals("ERROR", converter.transform(event(Level.ERROR), "ERROR"));
    }

    private LoggingEvent event(Level level) {
        LoggingEvent event = new LoggingEvent();
        event.setLevel(level);
        return event;
    }
}
