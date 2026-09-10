# TrustDeck Kafka Connector Plan

Status: planning checkpoint. This file is the only application-repository artifact in this bootstrap commit. No application code is implemented yet.

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

All paths are relative to the configured TrustDeck base URL and use client `1.0.1` services. `omitPrefix=false` is always used for creates.

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
| Static project/type preflight and entity CRUD | Project `project:read`; type `type:read`; entity type `entity:create`, `entity:read`, `entity:update`, and/or `entity:delete` for enabled operations. |
| Entity linkage review | `entity:record-linkage` only if the implementation uses the dedicated candidate endpoint; never grant or request `entity:resolve-linkage` because the connector does not auto-select/merge or use `CREATE_ORIGINAL`. |
| Entity-associated pseudonym inspection | `entity:list-pseudonyms`; associated domain `pseudonym:read`; `pseudonym:link` and target-domain `pseudonym:read`/`pseudonym:link` only if linked-pseudonym verification is explicitly configured. |
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
      group-id: ${KAFKA_GROUP_ID:ths-external-trustdeck}
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
  operation: ${TRUSTDECK_OPERATION:CREATE_PSEUDONYM}

mapping:
  domain-constant: ${TRUSTDECK_DOMAIN:}
  domain-path: ${KAFKA_MESSAGE_SOURCE_SYSTEM_DESCRIPTOR:/c_source_domain}
  identifier-path: ${KAFKA_MESSAGE_IDENTIFIER_DESCRIPTOR:/c_value}
  id-type-path: ${KAFKA_MESSAGE_IDTYPE_DESCRIPTOR:/c_type_of_pseudonym}
  psn-path: ${KAFKA_MESSAGE_PSN_DESCRIPTOR:/c_pseudonym}
  valid-from-path: ${KAFKA_MESSAGE_VALID_FROM_DESCRIPTOR:/t_timestamp}
  source-timezone: ${MAPPING_SOURCE_TIMEZONE:UTC}
  datetime-format: ${MAPPING_DATETIME_FORMAT:uuuu-MM-dd HH:mm:ss.SSS}

recovery:
  dlt-topic: ${KAFKA_DLT_TOPIC:${KAFKA_TOPIC}.DLT}
  review-topic: ${KAFKA_REVIEW_TOPIC:${KAFKA_TOPIC}.REVIEW}
```

The final classpath `application.yaml`, sanitized `.env.example`, mounted per-instance YAML examples, TLS truststore settings, all familiar `KAFKA_*` names, SMTP names, and WSL `set -a; source ./.env; set +a; ./mvnw spring-boot:run` instructions will be added after approval. Maven does not source `.env` automatically. The sample domain remains `/c_source_domain` (`datalake`) unless the confirmed configuration sets a constant; no source-system translation is invented. Generic defaults use UTC, while the concrete sample must use the confirmed timezone.

## Time and Input Rules

Offset/Z timestamps are converted by instant to UTC. Naive timestamps use a configured IANA zone and strict format, then become UTC wall-clock `LocalDateTime` values on the TrustDeck pseudonym wire. Ambiguous or DST-gap local times are rejected. JVM/container default timezone is UTC but is not relied upon. Date-only values and undeclared entity strings remain unchanged. Missing, JSON null, blank, wrong type, root arrays, primitives, invalid UTF-8/JSON, trailing values, excessive nesting, and oversized records are distinct validation failures. Bare top-level descriptors are accepted only through documented unambiguous conversion to a JSON Pointer.

## Failure, Offsets, and Recovery

Manual acknowledgment commits only after the HTTP operation or confirmed DLT/review publication. Earlier offsets in a partition are never skipped. Invalid/permanent record failures go promptly to a versioned DLT envelope; transient failures use bounded exponential retry and bounded `Retry-After`. 401/403, TLS, token, producer, and backend outages are classified separately; persistent systemic failures pause containers while polling/heartbeats continue. Work stays uncommitted, with bounded in-flight combined-stage state, and resumes after authenticated read-only probes recover. No automatic recovery-topic consumption, exactly-once HTTP claim, or blind whole-batch replay is made.

Review publication is terminal only after broker confirmation and uses `PENDING_LINKAGE_REVIEW`; ordinary 409 is not treated as linkage. Envelopes retain original key/value bytes, safe headers, source topic/partition/offset/timestamp, operation, target, sanitized category/status, attempts, and completed stage/UUID. Bearer tokens, cookies, passwords, arbitrary headers, and unsanitized payload logging are excluded. DLT/review topics need restricted ACLs/retention. Crash windows can duplicate HTTP effects or recovery publication; source topic/partition/offset is a deterministic downstream deduplication key.

## Dependencies and Licensing

Use Java 21, Spring Boot 3.5.16 and its managed Spring Kafka line, Jackson/Lombok, Actuator, validation, mail, the TrustDeck client, and Testcontainers/JUnit in test scope. Boot 3.5 is selected because it remains compatible with Spring Framework 6.2 used by client `1.0.1`; resolved dependency convergence will be checked. Direct dependency/plugin comments will state actual purpose and license. MIT/BSD/Apache-2.0 and permitted EPL-2.0 components are acceptable; no EPL component is described as Apache relicensed. The client runtime `slf4j-simple` must be excluded so Boot supplies one logging backend.

`org.trustdeck:client:1.0.1` is absent from Maven Central. It must be resolved from authenticated GitHub Packages using Maven settings, or from a pinned local source build of the exact client SHA until a published release is available. The connector will not depend on an unpublished client feature under `1.0.1`.

## Tests and Verification

Tests will exercise all seven operations against a controlled HTTP stub using the real client, including serialization/status 200/201/206, strict mapping and time conversion, leading zeros, update/delete selectors, combined automatic pseudonym behavior and stage-two replay, true linkage conflicts versus unrelated 409, ambiguous reads, and partial-batch correlation. Disposable uniquely named Kafka/Testcontainers resources will verify partition order, concurrency, manual offsets, DLT/review confirmation and producer failure, restart redelivery, outage pause/heartbeat/recovery, committed versus earliest offsets, bounded polls/large inputs, token refresh/expiry, and SMTP failure/suppression. No existing TrustDeck/PostgreSQL/Keycloak/Kafka service or production backlog is used.

Verification includes clean `./mvnw verify`, strict Javadocs including private members and test-source checks, dependency/license review, `git diff --check`, Compose config validation, Docker build without host Maven-cache reliance, and disposable readiness/liveness checks. Unavailable credentials/services will be reported as unverified with exact commands.

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

## Approval Questions

1. Confirm the timezone that produced the sample's offset-free `t_timestamp`; `Europe/Berlin` is only a proposed assumption. Confirm that this timestamp means pseudonym `validFrom`.
2. Provide one synthetic entity message plus its existing entity-type schema, and update/delete examples if available. Otherwise approve clearly labeled synthetic fixtures with explicit configurable mappings. Confirm required pseudonym update fields, including identifier/domain/value changes through `updateComplete`.
3. Confirm `/c_source_domain` as the example domain selector rather than `/c_target_domain`. The implementation supports any configured path and a constant override without translating source names or slash-containing values.
4. Approve or reject each client-library proposal independently:
   - Add optional finite HTTP connect/read timeout and transport configuration with backward-compatible constructors and safe defaults. Benefit: all client applications can avoid indefinite resource calls; connector-local alternative: an externally controlled network timeout, which cannot reliably bound the library's `RestClient` call.
   - No client credentials convenience API is proposed now because the connector can use `AccessTokenProvider` and standard OAuth client-credentials code. If that proves insufficient, it will be a separate proposal with caching, expiry skew, thread-safe refresh, finite token timeout, and cleanup.

General connector approval does not authorize edits to `trustdeck-client`; any approved client change gets a separate feature branch, tests, version, and published-or-pinned dependency decision.
