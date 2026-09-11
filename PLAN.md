# TrustDeck Kafka Connector Plan

Status: approved for implementation. Synthetic entity fixtures are test-only and do not define a production schema.

## Scope and Architecture

One Spring Boot application instance consumes one source topic with one fixed operation, project, mapping set, and group. It accepts exactly one bounded UTF-8 JSON object per record and calls the TrustDeck client synchronously on the listener thread. Partitions, not an extra executor, provide concurrency.

Proposed files/packages:

- `org.trustdeck.connector.kafka`: application entry point and package documentation.
- `config`: validated `@ConfigurationProperties`, Kafka/client/Actuator/SMTP beans.
- `mapping`: strict JSON parser, JSON Pointer mapping, target-path validation, explicit datetime conversion.
- `operation`: immutable mapped command models and seven-operation dispatcher/service.
- `trustdeck`: client/token adapter, preflight and bounded dynamic-target cache.
- `kafka`: consumer/producer factories, manual acknowledgment, pause/resume outage state, recovery/DLT/review envelopes.
- `notification`: optional bounded SMTP incident/recovery notifications.
- `src/main/resources/application.yaml`, `examples/`, `docker/`, `compose.yaml`, `compose.trustdeck-network.yaml`, `README.md`, `LICENSE`, `.env.example`, `.gitignore`, `.gitattributes`.
- Tests: unit mapping/time/operation tests, controlled HTTP-stub client contract tests, and disposable Kafka/Testcontainers integration tests.

The success path is centralized so a future result publisher can receive the source correlation and returned TrustDeck object without changing operation code. There is deliberately no result topic now.

## Operation Matrix

All paths are relative to the configured TrustDeck base URL and use the separately maintained client development build described below. `omitPrefix=false` is always used for creates.

| Operation | Client method and endpoint | Required mapping/behavior |
|---|---|---|
| `CREATE_PSEUDONYM` | `client.pseudonyms(domain).create(Pseudonym, false)`; `POST /api/domains/{domain}/pseudonyms` | `identifier`, `idType`; optional supplied `psn`, `validFrom`, `validTo`, `validityTime`; accept 200 existing and 201 created. |
| `UPDATE_PSEUDONYM` | `pseudonyms(oldDomain).update(PseudonymUpdate)` or approved `updateComplete`; `PUT /api/domains/{domain}/pseudonyms` or `/complete` | Explicit old selector, mapped validity fields, and only the approved complete-update fields. No implicit domain move or regeneration. |
| `DELETE_PSEUDONYM` | `pseudonyms(domain).delete(psn)` where supplied, otherwise exact identifier lookup then single delete | `DELETE /api/domains/{domain}/pseudonyms`; reject ambiguous identifier matches. Never call `deleteBatch()`, which deletes the whole domain. |
| `CREATE_ENTITY` | `entities(project, type).create(Entity)`; `POST /api/projects/{project}/entities/{type}` | Explicit mapped JSON object in `Entity.data`; no automatic linkage resolution. |
| `UPDATE_ENTITY` | `entities(project, type).update(UUID, Entity)`; `PUT /api/projects/{project}/entities/{type}/{uuid}` | Mapped TrustDeck UUID required; send validated replacement data and verify null/replacement semantics. |
| `DELETE_ENTITY` | `entities(project, type).delete(UUID)`; `DELETE /api/projects/{project}/entities/{type}/{uuid}` | Mapped UUID required; tombstoning is backend behavior. Already absent/deleted is success only after resource/type/project distinction. |
| `CREATE_ENTITY_AND_PSEUDONYM` | Entity `create`, then pseudonym `create` using returned UUID and `idType=TrustDeckID` | Inspect associated-domain pseudonym first; reuse/verify if it satisfies target, otherwise create target pseudonym. Preserve completed stage and UUID for bounded retry/replay; never roll back entity. |

Pseudonym create batches may be used only for contiguous compatible create records in the same domain/options and only after response correlation is proven. 206 bodies contain successes only; correlate by occurrence-aware identifying value and returned value, otherwise reconcile safely or fall back to singles. Repeated inputs, generated/imported values, reduced views, missing/duplicate responses, and ambiguous multi-pseudonym reads are never correlated by response index.

## Permissions

The service-account user is granted only the following resource permissions on configured resources. Backend authorization checks the JWT subject UUID, not a username field in the Kafka payload.

| Use | Exact permissions |
|---|---|
| Static project/type preflight and entity CRUD | Project `project:read`; type `type:read`; entity type `entity:create`, `entity:read`, `entity:update`, and/or `entity:delete` for enabled operations. `entity:create` also covers combined creation's first stage. |
| Entity linkage review | `entity:record-linkage` only if the implementation uses the dedicated candidate endpoint; never grant or request `entity:resolve-linkage` because the connector does not auto-select/merge or use `CREATE_ORIGINAL`. |
| Entity-associated pseudonym inspection | `entity:list-pseudonyms`; associated domain `pseudonym:read`; `pseudonym:link` and target-domain `pseudonym:read`/`pseudonym:link` only if linked-pseudonym verification is explicitly configured. Combined creation requires these reads only when associated-domain verification is enabled. |
| Pseudonym create | Target domain `pseudonym:create`; batch fallback additionally `pseudonym:create-batch`. |
| Pseudonym update | Old domain `pseudonym:update`; complete identifier/domain/value change additionally `pseudonym:update-complete`, plus target-domain authorization enforced by the backend. Batch update is not used until correlation/field support is verified; if enabled it needs `pseudonym:update-batch`. |
| Pseudonym delete/read | Domain `pseudonym:delete` and `pseudonym:read` for identifier lookup. `pseudonym:read-batch` and `pseudonym:delete-batch` are never required. |
| Domain target preflight | Each configured domain `domain:read`; dynamic domains use bounded expiring checks. No domain creation or global/admin permission. |
| Complete response/correlation | `complete-view` on each domain whose complete fields are required. It is additive to the operation permission, not a substitute. |

The connector will verify these grants through supported backend tooling during deployment testing. Projects, types, domains, schemas, and permissions are prerequisites; it creates none. REST audit records attribute the connector service account. Retaining mapped `t_user` in recovery provenance does not impersonate it.

## Configuration Example

```yaml
spring:
  application.name: trustdeck-kafka-connector
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS}
    consumer:
      group-id: ${KAFKA_GROUP_ID}
      enable-auto-commit: false
      auto-offset-reset: earliest
      max-poll-records: ${KAFKA_MAX_POLL_RECORDS:100}
  threads.virtual.enabled: false

trustdeck:
  service-url: ${TRUSTDECK_SERVICE_URL}
  token-endpoint: ${KEYCLOAK_TOKEN_ENDPOINT}
  client-id: ${KEYCLOAK_CLIENT_ID}
  client-secret: ${KEYCLOAK_CLIENT_SECRET}
  project: ${TRUSTDECK_PROJECT}
  operation: ${TRUSTDECK_OPERATION}

mapping:
  domain-constant: ${TRUSTDECK_DOMAIN:}
  domain-path: ${KAFKA_MESSAGE_SOURCE_SYSTEM_DESCRIPTOR:}
  identifier-path: ${KAFKA_MESSAGE_IDENTIFIER_DESCRIPTOR:}
  id-type-path: ${KAFKA_MESSAGE_IDTYPE_DESCRIPTOR:}
  psn-path: ${KAFKA_MESSAGE_PSN_DESCRIPTOR:}
  valid-from-path: ${KAFKA_MESSAGE_VALID_FROM_DESCRIPTOR:}
  source-timezone: ${MAPPING_SOURCE_TIMEZONE:UTC}
  datetime-format: ${MAPPING_DATETIME_FORMAT:}

recovery:
  dlt-topic: ${KAFKA_DLT_TOPIC:${KAFKA_TOPIC}.DLT}
  review-topic: ${KAFKA_REVIEW_TOPIC:${KAFKA_TOPIC}.REVIEW}
```

The classpath `application.yaml` is generic: it has no sample field, domain, project, entity-type, timestamp format, username, host, or credential defaults. The selected operation controls validation. Pseudonym operations do not require project/entity mappings; optional psn/validity/provenance mappings may be absent; a nonblank constant domain works without a domain path; missing required settings fail startup clearly. `examples/sample-pseudonym.yaml` explicitly maps `/c_source_domain`, `/c_value`, `/c_type_of_pseudonym`, `/c_pseudonym`, `/t_timestamp`, and `Europe/Berlin` with the sample format. Maven does not source `.env` automatically. The implementation supports any configured domain path and constant override without translation.

## Time and Input Rules

Offset/Z timestamps are converted by instant to UTC. Naive timestamps use a configured IANA zone and strict format, then become UTC wall-clock `LocalDateTime` values on the TrustDeck pseudonym wire. The approved sample uses `Europe/Berlin`; its `t_timestamp` is pseudonym `validFrom`. Ambiguous or DST-gap local times are rejected. Missing validity fields stay absent so backend defaults apply. JVM/container default timezone is UTC but is not relied upon. Date-only values and undeclared entity strings remain unchanged. Missing, JSON null, blank, wrong type, root arrays, primitives, invalid UTF-8/JSON, trailing values, excessive nesting, and oversized records are distinct validation failures. Bare top-level descriptors are accepted only through documented unambiguous conversion to a JSON Pointer.

## Failure, Offsets, and Recovery

Manual acknowledgment commits only after the HTTP operation or confirmed DLT/review publication. Earlier offsets in a partition are never skipped. Invalid/permanent record failures go promptly to a versioned DLT envelope; transient failures use bounded exponential retry and bounded `Retry-After`. 401/403, TLS, token, producer, and backend outages are classified separately; persistent systemic failures pause containers while polling/heartbeats continue. Work stays uncommitted, with bounded in-flight combined-stage state, and resumes after authenticated read-only probes recover. No automatic recovery-topic consumption, exactly-once HTTP claim, or blind whole-batch replay is made.

Review publication is terminal only after broker confirmation and uses `PENDING_LINKAGE_REVIEW`; ordinary 409 is not treated as linkage. Envelopes retain original key/value bytes, safe headers, source topic/partition/offset/timestamp, operation, target, sanitized category/status, attempts, and completed stage/UUID. Bearer tokens, cookies, passwords, arbitrary headers, and unsanitized payload logging are excluded. DLT/review topics need restricted ACLs/retention. Crash windows can duplicate HTTP effects or recovery publication; source topic/partition/offset is a deterministic downstream deduplication key.

## Dependencies and Licensing

Use Java 21, Spring Boot 3.5.16 and its managed Spring Kafka line, Jackson/Lombok, Actuator, validation, mail, the TrustDeck client development build, and Testcontainers/JUnit in test scope. Boot 3.5 is selected because it remains compatible with Spring Framework 6.2 used by the client. Direct dependency/plugin comments will state actual purpose and license. MIT/BSD/Apache-2.0 and permitted EPL-2.0 components are acceptable; no EPL component is described as Apache relicensed. The client runtime `slf4j-simple` must be excluded so Boot supplies one logging backend.

`org.trustdeck:client:1.0.2-SNAPSHOT` is a local development coordinate only and must not overwrite or impersonate released `1.0.1`. It contains the approved optional transport timeout feature: connect default 5 seconds and response-read default 30 seconds, with backward-compatible constructors. The sibling client change is on a separate feature branch, tested but unpublished. Maven and Docker use a reproducible pinned-source build/install path until publication; no client credentials convenience API is added.

## Tests and Verification

Tests will exercise all seven operations against a controlled HTTP stub using the real client, including serialization/status 200/201/206, strict mapping and time conversion, leading zeros, update/delete selectors, combined automatic pseudonym behavior and stage-two replay, true linkage conflicts versus unrelated 409, ambiguous reads, and partial-batch correlation. Disposable uniquely named Kafka/Testcontainers resources will verify partition order, concurrency, manual offsets, DLT/review confirmation and producer failure, restart redelivery, outage pause/heartbeat/recovery, committed versus earliest offsets, bounded polls/large inputs, token refresh/expiry, and SMTP failure/suppression. No existing TrustDeck/PostgreSQL/Keycloak/Kafka service or production backlog is used.

Verification includes clean `./mvnw verify`, strict Javadocs including private members and test-source checks, dependency/license review, `git diff --check`, Compose config validation, Docker build without host Maven-cache reliance, and disposable readiness/liveness checks. Unavailable credentials/services will be reported as unverified with exact commands.

## Implementation Checklist

This checklist is maintained against the complete approved scope, not just compilation.

### Implemented and verified

- Repository bootstrap, Apache license, generic configuration, sample mapping, Docker/Compose files, instance template, and baseline documentation.
- Java 21/Spring Boot application, typed configuration, JSON object mapping, strict timestamp conversion, domain constant precedence, and seven operation names.
- TrustDeck client adapter with OAuth client-credentials token caching and finite connect/read timeout configuration.
- Real-client HTTP dispatch tests cover all seven operation branches; mapper/listener/batch/configuration/health tests plus disposable Kafka and recovery publisher tests bring the clean verification suite to 22 passing tests.
- Strict Javadocs, `git diff --check`, Compose config validation, Docker build, isolated container liveness/readiness probes, and client `mvn clean verify` pass.
- Client timeout change is isolated on `feature/http-timeouts`, commit `5ca64d7`, with the client test suite previously passing 73 tests.

### Implemented but not verified

- Pseudonym batch request construction, byte limit enforcement, and occurrence-aware response matching.
- Known-correlated HTTP 206 partial responses fall back to singles for unresolved inputs; unsafe/reduced responses publish `UNKNOWN_OUTCOME` envelopes with bounded guidance, but publisher failure and broker-offset behavior remain unverified.
- Combined entity/pseudonym dispatch with bounded in-memory completed-stage state and confirmed recovery UUID fields.
- Batch Kafka listener wiring, byte-preserving recovery envelope serialization, readiness/liveness groups, outage pause/probe/resume coordinator, and notification integration.
- Docker builder path for the pinned client development commit, verified with Docker daemon.
- Strict Javadoc execution and current handwritten declaration documentation.

### Missing or incomplete

- Retry-after handling, producer-outage classification, and verification of terminal handling for unsafe batch outcomes.
- Full authenticated static/dynamic preflight/resource checks and retry-after handling.
- Recovery producer-failure tests, full target identity, and replay execution from stage/UUID metadata.
- Meaningful Kafka/Testcontainers restart/producer-failure/outage/offset integration tests and unsafe partial 206 tests. A disposable Kafka round-trip now proves isolated broker creation and source byte transport.
- Token-expiry/refresh and SMTP cooldown/failure tests.
- Exact HTTP path/payload assertions, ambiguous-read/linkage-conflict tests, and target-path validation tests.
- Dependency/license report and complete operation-specific environment documentation.

### Blocked

- Live backend permission preflight and end-to-end TrustDeck verification are blocked until isolated synthetic development credentials/resources are available.
- Disposable Kafka and Docker runtime checks are blocked if the local Docker daemon is unavailable; this will be tested explicitly rather than using development services.

### Requirement Evidence

- Seven operations: implementation `OperationService`; behavioral test `OperationServiceHttpTest`; exact path/payload assertions are still missing.
- Mapping/timezone: implementation `JsonMessageMapper`; behavioral test `JsonMessageMapperTest`; malformed-input breadth and target-path validation remain missing.
- Batch/206: implementation `PseudonymBatchService`; behavioral test `PseudonymBatchServiceTest`; unsafe/reduced response and broker outcome tests remain missing.
- Offsets/recovery: implementation `ConnectorListener`, `RecoveryPublisher`, and `RecoveryEnvelope`; behavioral tests `ConnectorListenerTest` and `RecoveryPublisherTest`; `KafkaResourceIntegrationTest` proves disposable broker transport, but real broker commit/restart and producer-failure tests are missing.
- Combined/replay: implementation `OperationService` and recovery metadata; no behavioral replay test exists yet.
- Outage/health: implementation `OutageCoordinator` and `ConnectorHealthIndicator`; `HealthEndpointTest` proves HTTP endpoints, but pause/resume with a broker is untested.
- Configuration: implementation `ConfigurationValidation`; behavioral test `ConfigurationValidationTest`; static/dynamic authenticated preflight is missing.
- Client timeout/token: implementation sibling client commit `5ca64d7`; client suite proves timeout behavior and endpoint contracts, but connector token-expiry integration is missing.
- SMTP: implementation `NotificationService`/`OutageCoordinator`; no suppression/failure behavioral test exists.

### Next concrete action

Add broker-backed offset/restart/producer-failure tests; then implement exact replay execution from confirmed combined-stage envelopes.

## Proposed Commits

1. `chore: bootstrap planning and repository metadata`
2. `feat: add validated mappings and TrustDeck operations`
3. `feat: add Kafka recovery, offsets, health, and notifications`
4. `test: add HTTP contract and disposable Kafka integration coverage`
5. `docs: add deployment examples, permissions, replay, and license report`

## References

- Backend `main`: `31f63cee9d8c2659148cfb05ac3de374277ffcb1`.
- Client `main`: `22c02fe16db97839afaee6170afcf6b6e0639aac`; declared version `org.trustdeck:client:1.0.1`.
- Frontend `main`: `5c86a0e81ef7360dfe3e0bf440169c922e3a361a`.
- Spring Boot Kafka reference: `https://docs.spring.io/spring-boot/3.5.16/reference/messaging/kafka.html`.
- Spring Kafka listener/acknowledgment reference: `https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/message-listener-container.html`.

## Unsupported or Unreliable Behavior

The current API cannot reliably provide original Kafka-user REST audit attribution, strict exactly-once effects, safe generic entity update/delete without a mapped TrustDeck UUID, or automatic resolution of record-linkage conflicts. Identifier-only pseudonym reads/deletes are inherently ambiguous when a domain allows multiple pseudonyms. Batch response bodies do not contain enough guaranteed correlation information for every generated/imported/repeated input, so singles or explicit unknown outcomes are required. The backend's mixed local-clock/UTC comparisons and timezone-free `LocalDateTime` wire contract cannot be completely repaired by connector conversion.

## Approved Decisions

- The sample's `t_timestamp` is `validFrom` from `Europe/Berlin`; explicit conversion preserves the instant and rejects DST ambiguity/gaps.
- No production entity schema was supplied. Synthetic entity messages and matching schemas are clearly labeled test fixtures with configurable mappings only.
- All seven operations remain in scope. Pseudonym validity and explicitly mapped identifier/domain/value changes use the backend-supported standard or complete client method; nothing moves or regenerates implicitly.
- `/c_source_domain` is the sample selector. A nonblank configured constant wins; generic configuration has no sample-specific default.
- The timeout-only client feature is approved. No client-credentials convenience API or unrelated transport framework is approved.
