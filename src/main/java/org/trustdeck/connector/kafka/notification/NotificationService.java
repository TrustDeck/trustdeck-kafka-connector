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

package org.trustdeck.connector.kafka.notification;

import java.util.List;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;

/** Sends optional, sanitized operational email notifications without affecting offsets. */
@Service
public class NotificationService {

    /** Spring mail sender, when configured. */
    private final ObjectProvider<JavaMailSender> sender;

    /** Recipient list. */
    private final List<String> recipients;

    /** Optional sender address. */
    private final String from;

    /** Configured subject prefix. */
    private final String subjectPrefix;

    /** Minimum interval between repeated non-recovery subjects. */
    private final Duration cooldown;

    /** Last attempted notification by subject. */
    private final ConcurrentMap<String, Instant> lastSent = new ConcurrentHashMap<>();

    /**
     * Creates a notification service.
     *
     * @param sender mail sender
     * @param recipients configured recipients
     * @param from configured sender
     * @param subjectPrefix configured subject prefix
     */
    public NotificationService(ObjectProvider<JavaMailSender> sender,
                               @org.springframework.beans.factory.annotation.Value("${SMTP_TO:}") String recipients,
                               @org.springframework.beans.factory.annotation.Value("${SMTP_FROM:}") String from,
                               @org.springframework.beans.factory.annotation.Value("${SUBJECT_PREFIX:[TrustDeck Kafka Connector]}") String subjectPrefix) {
        this(sender, recipients, from, subjectPrefix, Duration.ofMinutes(15));
    }

    /** Creates a notification service with an explicit repeat-notification cooldown.
     * @param sender mail sender
     * @param recipients configured recipients
     * @param from configured sender
     * @param subjectPrefix configured subject prefix
     * @param cooldown repeat-notification interval
     */
    @org.springframework.beans.factory.annotation.Autowired
    public NotificationService(ObjectProvider<JavaMailSender> sender,
                               @org.springframework.beans.factory.annotation.Value("${SMTP_TO:}") String recipients,
                               @org.springframework.beans.factory.annotation.Value("${SMTP_FROM:}") String from,
                               @org.springframework.beans.factory.annotation.Value("${SUBJECT_PREFIX:[TrustDeck Kafka Connector]}") String subjectPrefix,
                               @org.springframework.beans.factory.annotation.Value("${SMTP_COOLDOWN:15m}") Duration cooldown) {
        this.sender = sender;
        this.recipients = recipients == null ? List.of() : java.util.Arrays.stream(recipients.split(","))
                .map(String::trim).filter(value -> !value.isBlank()).toList();
        this.from = from;
        this.subjectPrefix = subjectPrefix;
        this.cooldown = cooldown == null || cooldown.isNegative() ? Duration.ZERO : cooldown;
    }

    /**
     * Sends a sanitized notification when recipients are configured.
     *
     * @param subject safe subject
     * @param body safe body
     */
    public void send(String subject, String body) {
        if (recipients.isEmpty()) {
            return;
        }
        boolean recovery = subject != null && subject.toLowerCase(java.util.Locale.ROOT).contains("recovered");
        Instant now = Instant.now();
        if (!recovery && cooldown.compareTo(Duration.ZERO) > 0) {
            Instant previous = lastSent.putIfAbsent(subject, now);
            if (previous != null && previous.plus(cooldown).isAfter(now)) {
                return;
            }
            if (previous != null) {
                lastSent.replace(subject, previous, now);
            }
        }
        try {
            SimpleMailMessage message = new SimpleMailMessage();
            message.setTo(recipients.toArray(String[]::new));
            if (from != null && !from.isBlank()) {
                message.setFrom(from);
            }
            message.setSubject((subjectPrefix == null ? "" : subjectPrefix) + " " + subject);
            message.setText(body);
            JavaMailSender mailSender = sender.getIfAvailable();
            if (mailSender != null) {
                mailSender.send(message);
            }
        } catch (RuntimeException ignored) {
            // Mail is advisory and must never acknowledge or block Kafka work.
        }
    }
}
