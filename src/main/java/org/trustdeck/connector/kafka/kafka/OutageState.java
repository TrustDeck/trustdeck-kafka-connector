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

import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.stereotype.Component;

/** Small shared state used by readiness and listener recovery handling. */
@Component
public class OutageState {

    /** Creates initially healthy connector state. */
    public OutageState() {
    }

    /** Current paused state. */
    private final AtomicBoolean paused = new AtomicBoolean();

    /** Last sanitized reason. */
    private volatile String reason = "";

    /**
     * Marks the connector paused.
     *
     * @param value whether paused
     * @param newReason sanitized reason
     */
    public void setPaused(boolean value, String newReason) {
        reason = newReason == null ? "" : newReason;
        paused.set(value);
    }

    /**
     * Returns whether consumption is paused.
     *
     * @return true when paused
     */
    public boolean isPaused() {
        return paused.get();
    }

    /**
     * Returns the sanitized pause reason.
     *
     * @return pause reason
     */
    public String getReason() {
        return reason;
    }
}
