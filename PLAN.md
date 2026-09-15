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
- `src/main/resources/application.yaml`, `src/main/resources/example-pseudonymization.yaml`, `docker/`, `README.md`, `LICENSE`, `.env.example`, `.gitignore`, `.gitattributes`.
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

The classpath `application.yaml` is generic: it has no sample field, domain, project, entity-type, timestamp format, username, host, or credential defaults. The selected operation controls validation. Pseudonym operations do not require project/entity mappings; optional psn/validity/provenance mappings may be absent; a nonblank constant domain works without a domain path; missing required settings fail startup clearly. `src/main/resources/example-pseudonymization.yaml` explicitly maps `/c_source_domain`, `/c_value`, `/c_type_of_pseudonym`, `/c_pseudonym`, `/t_timestamp`, and `Europe/Berlin` with the sample format. Maven does not source `.env` automatically. The implementation supports any configured domain path and constant override without translation.

## Time and Input Rules

Offset/Z timestamps are converted by instant to UTC. Naive timestamps use a configured IANA zone and strict format, then become UTC wall-clock `LocalDateTime` values on the TrustDeck pseudonym wire. The approved sample uses `Europe/Berlin`; its `t_timestamp` is pseudonym `validFrom`. Ambiguous or DST-gap local times are rejected. Missing validity fields stay absent so backend defaults apply. JVM/container default timezone is UTC but is not relied upon. Date-only values and undeclared entity strings remain unchanged. Missing, JSON null, blank, wrong type, root arrays, primitives, invalid UTF-8/JSON, trailing values, excessive nesting, and oversized records are distinct validation failures. Bare top-level descriptors are accepted only through documented unambiguous conversion to a JSON Pointer.

## Failure, Offsets, and Recovery

Manual acknowledgment commits only after the HTTP operation or confirmed DLT/review publication. Earlier offsets in a partition are never skipped. Invalid/permanent record failures go promptly to a versioned DLT envelope; transient failures use bounded exponential retry and bounded `Retry-After`. 401/403, TLS, token, producer, and backend outages are classified separately; persistent systemic failures pause containers while polling/heartbeats continue. Work stays uncommitted, with bounded in-flight combined-stage state, and resumes after authenticated read-only probes recover. No automatic recovery-topic consumption, exactly-once HTTP claim, or blind whole-batch replay is made.

Review publication is terminal only after broker confirmation and uses `PENDING_LINKAGE_REVIEW`; ordinary 409 is not treated as linkage. Envelopes retain original key/value bytes, safe headers, source topic/partition/offset/timestamp, operation, target, sanitized category/status, attempts, and completed stage/UUID. Bearer tokens, cookies, passwords, arbitrary headers, and unsanitized payload logging are excluded. DLT/review topics need restricted ACLs/retention. Crash windows can duplicate HTTP effects or recovery publication; source topic/partition/offset is a deterministic downstream deduplication key.

## Dependencies and Licensing

Use Java 21, Spring Boot 3.5.16 and its managed Spring Kafka line, Jackson/Lombok, Actuator, validation, mail, the TrustDeck client development build, and Testcontainers/JUnit in test scope. Boot 3.5 is selected because it remains compatible with Spring Framework 6.2 used by the client. Direct dependency/plugin comments will state actual purpose and license. MIT/BSD/Apache-2.0 and permitted EPL-2.0 components are acceptable; no EPL component is described as Apache relicensed. The client runtime `slf4j-simple` must be excluded so Boot supplies one logging backend.

`org.trustdeck:client:1.0.2-SNAPSHOT` is a local development coordinate only and must not overwrite or impersonate released `1.0.1`. It contains the approved optional transport timeout and `Retry-After` response features: connect default 5 seconds and response-read default 30 seconds, with backward-compatible constructors. The sibling client change is on a separate feature branch, tested but unpublished. Maven and Docker use reproducible pinned-source build/install path at client commit `0133ed9` until publication; no client credentials convenience API is added.

## Tests and Verification

Tests will exercise all seven operations against a controlled HTTP stub using the real client, including serialization/status 200/201/206, strict mapping and time conversion, leading zeros, update/delete selectors, combined automatic pseudonym behavior and stage-two replay, true linkage conflicts versus unrelated 409, ambiguous reads, and partial-batch correlation. Disposable uniquely named Kafka/Testcontainers resources will verify partition order, concurrency, manual offsets, DLT/review confirmation and producer failure, restart redelivery, outage pause/heartbeat/recovery, committed versus earliest offsets, bounded polls/large inputs, token refresh/expiry, and SMTP failure/suppression. No existing TrustDeck/PostgreSQL/Keycloak/Kafka service or production backlog is used.

Verification includes clean `mvn verify`, strict Javadocs including private members and test-source checks, dependency/license review (`mvn org.codehaus.mojo:license-maven-plugin:2.7.0:add-third-party -Dlicense.includedScopes=compile,runtime`), `git diff --check`, Compose config validation, Docker build without host Maven-cache reliance, and disposable readiness/liveness checks. Unavailable credentials/services will be reported as unverified with exact commands.

## Implementation Checklist

This checklist is maintained against the complete approved scope, not just compilation.

### Implemented and verified

- Repository bootstrap, Apache license, generic configuration, sample mapping, Docker/Compose files, instance template, and baseline documentation.
- Java 21/Spring Boot application, typed configuration, JSON object mapping, strict timestamp conversion, domain constant precedence, and seven operation names.
- TrustDeck client adapter with OAuth client-credentials token caching, finite connect/read timeout configuration, and bounded `Retry-After` propagation.
- Real-client HTTP dispatch tests cover all seven operation branches; mapper/listener/batch/configuration/health/token/replay tests plus disposable Kafka, fresh-context restart, and recovery publisher/notification tests pass. `mvn clean verify` passes with 47 tests.
- Strict Javadocs, `git diff --check`, Compose config validation, Docker build, isolated container liveness/readiness probes, and client `mvn clean verify` pass.
- Client timeout and `Retry-After` changes are isolated on `feature/http-timeouts`, with pinned build commit `0133ed9` and the client test suite passing 74 tests.

### Implemented but not verified

- Pseudonym batch request construction, byte limit enforcement, and occurrence-aware response matching.
- Known-correlated HTTP 206 partial responses fall back to singles for unresolved inputs; unsafe/reduced responses publish `UNKNOWN_OUTCOME` envelopes with bounded guidance, and recovery publisher failure propagation is unit-tested, but broker-offset behavior remains unverified.
- Combined entity/pseudonym dispatch with bounded in-memory completed-stage state and confirmed recovery UUID fields.
- Batch Kafka listener wiring, byte-preserving recovery envelope serialization, readiness/liveness groups, authenticated static/dynamic preflight, outage pause/probe/resume coordinator, SMTP cooldown, and notification integration.
- No additional verification is pending for the Docker builder path, strict Javadocs, or isolated container liveness/readiness probes; all pass in the current implementation.

### Missing or incomplete

- Verification of terminal handling for unsafe batch outcomes remains incomplete at the broker boundary.
- Broker-level recovery producer-failure tests and replay execution from stage/UUID metadata after a separate JVM process remain incomplete.
- `ConnectorRestartIntegrationTest.preservesCommittedPrefixAndRedeliversBlockedRecord` proves fresh Spring connector contexts, retained broker/group state, committed-prefix preservation, blocked-record offset retention, redelivery, and eventual commit. `ConnectorKafkaIntegrationTest` proves real listener processing, HTTP effect, confirmed DLT publication, two-container concurrency, and broker-observed commits. Real recovery-producer outage, broker unsafe-206, and abrupt process-kill tests remain incomplete.
- Remaining exact HTTP path/payload assertions and ambiguous-read/linkage-conflict tests; pseudonym-create path/payload and batch correlation coverage are complete.
- Dependency/license report and complete operation-specific environment documentation are documented; the report generates successfully with the unpublished client license metadata recognized.

### Environment-dependent verification

- Live backend permission preflight and end-to-end TrustDeck verification require isolated synthetic development credentials/resources.
- Disposable Kafka and Docker runtime checks have passed with the local Docker daemon; this is no longer a current blocker.

### Requirement Evidence

- Seven operations: implementation `OperationService`; behavioral test `OperationServiceHttpTest` covers all dispatch branches, combined stage-two replay, and exact pseudonym path/payload assertions; remaining operation payload assertions are missing.
- Mapping/timezone: implementation `JsonMessageMapper`; behavioral test `JsonMessageMapperTest` covers validity-time mapping and omission, malformed UTF-8, trailing JSON, invalid roots, and DST gap/overlap behavior; target-path validation is covered by `ConfigurationValidationTest`.
- Batch/206: implementation `PseudonymBatchService`; behavioral test `PseudonymBatchServiceTest` covers known partial fallback, reduced/unmatched responses, reordered and repeated occurrence correlation, UTF-8 limits, and listener terminal classification; broker outcome tests remain missing.
- Offsets/recovery: implementation `ConnectorListener`, `RecoveryPublisher`, and `RecoveryEnvelope`; behavioral tests cover recovery producer propagation and source-ack protection, while `ConnectorKafkaIntegrationTest` proves real listener processing, confirmed DLT publication, and broker-observed commits; broker restart and producer-failure tests are missing.
- Combined/replay: implementation `OperationService`, `ManualReplayService`, and guarded `ManualReplayController`; `OperationServiceHttpTest.replaysCombinedOperationWithoutCreatingAnotherEntity` and `replayUsesPreservedValueAcrossProcessBoundary` prove UUID restoration and preserved-byte validation. Separate-JVM replay execution remains missing.
- Outage/health: implementation `OutageCoordinator`, `TrustDeckPreflight`, and `ConnectorHealthIndicator`; `OutageCoordinatorTest.probesFailedDynamicTargetsBeforeResume` proves target-aware authenticated recovery probing, while `ConnectorKafkaIntegrationTest` exercises authenticated startup preflight and `HealthEndpointTest` proves HTTP endpoints. Pause/heartbeat/resume with a live broker remains untested.
- Configuration: implementation `ConfigurationValidation`; behavioral test `ConfigurationValidationTest`; `ConnectorKafkaIntegrationTest` exercises authenticated startup and dynamic-target preflight against the HTTP stub.
- Client timeout/token: implementation sibling client pinned commit `0133ed9`; client suite proves timeout behavior, endpoint contracts, and `Retry-After` propagation, while `ClientCredentialsTokenProviderTest` proves expiry refresh against a controlled token endpoint.
- SMTP: implementation `NotificationService`/`OutageCoordinator`; `NotificationServiceTest` proves recipient suppression, transport-failure isolation, cooldown suppression, and uncapped recovery notification.

### Next concrete action

Add real recovery-producer fault injection and separate-JVM replay only if the deployment test harness can isolate the producer transport and expose the guarded operator endpoint; the locally runnable fresh-context offset/replay and unit evidence are complete.

### A-G Evidence Matrix

| Section | Implementation and evidence | Observed result | Remaining boundary |
|---|---|---|---|
| A | `ConnectorRestartIntegrationTest.preservesCommittedPrefixAndRedeliversBlockedRecord`; `ConnectorKafkaIntegrationTest` | Fresh contexts retain group offsets; source HTTP markers are `committed`, `blocked`, `blocked`; blocked next-offset stays at the committed prefix; later commit advances after recovery. Repeated twice. | Context restart is not an abrupt JVM/process kill; multi-record blocked batch is not separately exercised. |
| B | `ConnectorListenerTest.pausesWhenRecoveryPublicationFails`; `RecoveryPublisherTest` | Source acknowledgment is withheld and outage state is requested when publication fails. | No real broker ACL/network isolation test for DLT and review publication. |
| C | `ManualReplayService`, `ManualReplayController`, `OperationServiceHttpTest.replaysCombinedOperationWithoutCreatingAnotherEntity`, `replayUsesPreservedValueAcrossProcessBoundary` | Preserved `valueBase64` is decoded, mismatched caller bytes are rejected, completed UUID restores stage two, and no second entity create occurs. | No separate JVM/container operator invocation test. |
| D | `PseudonymBatchServiceTest` correlation/limit tests; listener batch classification test | Known partial success is not resubmitted; reduced and unmatched responses are rejected; reordered and repeated occurrences correlate safely; serialized UTF-8 byte limits are enforced; deterministic rejection routes to DLT. | No broker-backed unsafe-206 publication/offset test. |
| E | `OutageCoordinatorTest.probesFailedDynamicTargetsBeforeResume`, preflight integration, health and notification tests | Dynamic failed targets are rechecked before resume; health and SMTP isolation tests pass. | No live broker pause/heartbeat/resume outage test. |
| F | `OperationServiceHttpTest` (10 tests), `JsonMessageMapperTest` (8 tests), client suite evidence | All seven operation branches, validity-time serialization, selectors, preserved types, malformed UTF-8, trailing JSON, roots, and DST gap/overlap behavior pass. | Remaining exact payload/status and backend ambiguity cases are not fully covered. |
| G | `mvn clean verify`; license report; both Compose configs; isolated Docker build | 47 tests, no skips, Javadocs, Compose validation, dependency report review, and clean pinned-client Docker build pass. | Live backend permissions/resources remain deployment-only. |

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
