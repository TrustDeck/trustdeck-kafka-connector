/*
 * TrustDeck Kafka Connector
 * Copyright 2026 Armin Müller
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.trustdeck.connector.kafka.notification;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import java.time.Duration;

/** Verifies advisory SMTP notification behavior. */
class NotificationServiceTest {

    /** Ensures notifications are suppressed when no recipients are configured. */
    @Test
    void suppressesNotificationWithoutRecipients() {
        @SuppressWarnings("unchecked")
		ObjectProvider<JavaMailSender> provider = mock(ObjectProvider.class);
        JavaMailSender sender = mock(JavaMailSender.class);
        when(provider.getIfAvailable()).thenReturn(sender);

        new NotificationService(provider, "", "from@example.test", "[Test]").send("Subject", "Body");

        verify(sender, never()).send(any(SimpleMailMessage.class));
    }

    /** Ensures a mail transport failure does not escape into Kafka processing. */
    @Test
    void swallowsNotificationTransportFailure() {
        @SuppressWarnings("unchecked")
		ObjectProvider<JavaMailSender> provider = mock(ObjectProvider.class);
        JavaMailSender sender = mock(JavaMailSender.class);
        when(provider.getIfAvailable()).thenReturn(sender);
        doThrow(new IllegalStateException("SMTP unavailable")).when(sender)
                .send(any(SimpleMailMessage.class));

        assertDoesNotThrow(() -> new NotificationService(provider, "to@example.test", "from@example.test", "[Test]")
                .send("Subject", "Body"));
        verify(sender).send(any(SimpleMailMessage.class));
    }

    /** Ensures repeated outage notices are cooled down while recovery is always sent. */
    @Test
    void coolsDownOutageButNotRecoveryNotification() {
        @SuppressWarnings("unchecked")
		ObjectProvider<JavaMailSender> provider = mock(ObjectProvider.class);
        JavaMailSender sender = mock(JavaMailSender.class);
        when(provider.getIfAvailable()).thenReturn(sender);
        NotificationService notifications = new NotificationService(provider, "to@example.test", "from@example.test",
                "[Test]", Duration.ofHours(1));

        notifications.send("TrustDeck outage", "first");
        notifications.send("TrustDeck outage", "duplicate");
        notifications.send("TrustDeck recovered", "recovered");

        verify(sender, times(2)).send(any(SimpleMailMessage.class));
    }
}
