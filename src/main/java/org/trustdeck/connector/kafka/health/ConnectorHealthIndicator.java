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

package org.trustdeck.connector.kafka.health;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;
import org.trustdeck.connector.kafka.kafka.OutageState;

/** Reports readiness-impacting connector pause state without exposing sensitive details. */
@Component("connector")
public class ConnectorHealthIndicator implements HealthIndicator {

    /** Shared pause state. */
    private final OutageState state;

    /**
     * Creates the indicator.
     *
     * @param state outage state
     */
    public ConnectorHealthIndicator(OutageState state) {
        this.state = state;
    }

    /**
     * Returns readiness health.
     *
     * @return health status
     */
    @Override
    public Health health() {
        if (state.isPaused()) {
            return Health.down().withDetail("state", "paused").build();
        }
        return Health.up().withDetail("state", "running").build();
    }
}
