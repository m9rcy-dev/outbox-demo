# Spring Transactional Outbox Demo

A production-grade reference implementation of the **Transactional Outbox Pattern** for Spring Boot, built for multi-pod OpenShift/Kubernetes deployments. Demonstrates how to drive multi-step downstream workflows reliably — with atomic writes, resumable checkpointing, structured logging, and a layered concurrency model — without a message broker.

---

## Table of Contents

- [The Problem This Solves](#the-problem-this-solves)
- [Architecture Overview](#architecture-overview)
- [The Card Application Pipeline](#the-card-application-pipeline)
- [Reliability Design](#reliability-design)
  - [Atomic Write at Submission](#atomic-write-at-submission)
  - [SKIP LOCKED: Pod Distribution](#skip-locked-pod-distribution)
  - [Per-Event REQUIRES_NEW Transactions](#per-event-requires_new-transactions)
  - [saveAndFlush: Eager Version Checks](#saveandflush-eager-version-checks)
  - [@Version: Optimistic Locking](#version-optimistic-locking)
  - [Idempotency Re-fetch Guard](#idempotency-re-fetch-guard)
  - [Step Checkpointing and Resume](#step-checkpointing-and-resume)
  - [Retry and Permanent Failure](#retry-and-permanent-failure)
- [Observability and Logging](#observability-and-logging)
  - [Structured MDC Fields](#structured-mdc-fields)
  - [Log Pattern](#log-pattern)
  - [Async MDC Propagation](#async-mdc-propagation)
- [Data Model](#data-model)
- [Configuration Reference](#configuration-reference)
- [Adding a New Pipeline](#adding-a-new-pipeline)
- [Test Strategy](#test-strategy)
  - [Tier 1 — Unit Tests (Mockito)](#tier-1--unit-tests-mockito)
  - [Tier 2 — H2 Integration Tests (WireMock)](#tier-2--h2-integration-tests-wiremock)
  - [Tier 3 — PostgreSQL Concurrency Tests (Testcontainers)](#tier-3--postgresql-concurrency-tests-testcontainers)
- [Running the Tests](#running-the-tests)
- [Running Locally](#running-locally)

---

## The Problem This Solves

When a business operation requires calling external APIs (credit bureau, card provider, notification service), a naive approach calls them directly inside the HTTP handler:

```
POST /applications → save entity → call API 1 → call API 2 → call API 3 → return
```

This breaks under failure in multiple ways:

| Failure point | Consequence |
|---|---|
| API 1 times out | No side effects — safe to retry |
| App crashes after API 1 succeeds but before API 2 | API 1 was called; retrying re-calls it |
| App crashes after all 3 APIs succeed but before DB commit | All 3 APIs were called, no record of it |
| Two pods receive the same retry | Each calls all APIs independently |

The Outbox Pattern eliminates this by separating the **commit** from the **execution**:

1. The HTTP handler atomically writes the business entity AND an `outbox_event` row in one transaction — no API calls.
2. A background poller reads `PENDING` events and drives them through a pipeline of steps.
3. Each step's result is checkpointed back to the outbox row before moving to the next step.
4. Crashes are safe: the next poll resumes from the last checkpoint.
5. Concurrency is safe: `SKIP LOCKED` + `@Version` prevent two pods from committing the same step twice.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│  HTTP Layer                                                      │
│  CardApplicationController → CardApplicationService             │
│         ↓  one @Transactional write                              │
│  ┌──────────────┐   ┌───────────────────┐                       │
│  │card_application│  │  outbox_event     │  written atomically   │
│  │status=SUBMITTED│  │  status=PENDING   │                       │
│  └──────────────┘   └───────────────────┘                       │
└─────────────────────────────────────────────────────────────────┘
                           ↓  async (every 5 s)
┌─────────────────────────────────────────────────────────────────┐
│  Scheduler Layer                                                 │
│  PipelineOutboxPoller                                            │
│    └─ findPendingForProcessing (SELECT FOR UPDATE SKIP LOCKED)   │
│         ↓  for each event (REQUIRES_NEW per event)              │
│    OutboxEventProcessor.process()                                │
│         ↓  re-fetch + PENDING guard                              │
│    PipelineRegistry.getSteps(pipelineType)                       │
│         ↓  execute steps in order from currentStep              │
│    Step 0 → saveAndFlush(checkpoint) → Step 1 → ... → PROCESSED │
└─────────────────────────────────────────────────────────────────┘
                           ↓  each step calls
┌─────────────────────────────────────────────────────────────────┐
│  External APIs (stubbed in tests via WireMock)                   │
│  CreditBureauClient  CardProviderClient  NotificationClient      │
└─────────────────────────────────────────────────────────────────┘
```

### Key classes

| Class | Responsibility |
|---|---|
| `CardApplicationService` | Atomic HTTP-layer write: save entity + outbox event in one tx |
| `OutboxService` | Creates `OutboxEvent` rows; validates the pipeline type against the registry |
| `PipelineOutboxPoller` | Scheduled poller; fetches batch with `SKIP LOCKED`; drives per-event processing |
| `OutboxEventProcessor` | Processes one event in a `REQUIRES_NEW` transaction; handles checkpointing, retries, failure |
| `PipelineRegistry` | Auto-discovers all `PipelineStep` beans at startup; organises them by type and index |
| `PipelineStep<C>` | Interface for a single pipeline step |
| `MdcContext` | `AutoCloseable` MDC holder; propagates correlation IDs through threads |

---

## The Card Application Pipeline

The demo implements a four-step credit card application workflow:

```
Step 0: CreditBureauCheckStep
  → POST /credit/check
  → sets ctx.creditScore
  → updates CardApplication.status = CREDIT_CHECKED

Step 1: CardProviderRegisterStep
  → POST /cards/register  (uses ctx.creditScore)
  → sets ctx.providerRef
  → updates CardApplication.status = PROVIDER_REGISTERED

Step 2: NotificationStep
  → POST /notifications/send  (uses ctx.providerRef)
  → sets ctx.notificationId
  → updates CardApplication.status = NOTIFIED

Step 3: FinalDbUpdateStep
  → no external API call
  → updates CardApplication.status = COMPLETED
```

Each step receives a **shared mutable context** (`CardApplicationContext`) that is serialised to JSON and persisted as `outbox_event.payload` after every step. If the application restarts mid-pipeline, the next poll deserialises the context from the checkpoint and resumes from the step that was not yet completed.

The context carries outputs forward through the pipeline:

```
submit      → {applicationId, applicantName, email, annualIncome}
after step 0 → + creditScore
after step 1 → + providerRef
after step 2 → + notificationId
after step 3 → pipeline complete
```

---

## Reliability Design

Six independent mechanisms layer together to make processing safe under pod restarts, network failures, and concurrent deployments.

### Atomic Write at Submission

`CardApplicationService.submitApplication` runs in a single `@Transactional` block:

```java
@Transactional
public CardApplication submitApplication(...) {
    CardApplication app = cardApplicationRepository.save(...);   // business row
    outboxService.saveEvent(PIPELINE_TYPE, app.getId(), context); // outbox row
    return app;
}
```

Both rows are in the same transaction. If anything fails before the commit — including a JVM crash — neither row is written. The HTTP caller gets an error and can retry safely. There is no window where the entity exists but the outbox event does not.

### SKIP LOCKED: Pod Distribution

The batch fetch query uses `SELECT FOR UPDATE SKIP LOCKED`:

```java
@Lock(LockModeType.PESSIMISTIC_WRITE)
@QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = "-2"))
@Query("SELECT e FROM OutboxEvent e WHERE e.status = 'PENDING' ORDER BY e.createdAt ASC LIMIT :limit")
List<OutboxEvent> findPendingForProcessing(@Param("limit") int limit);
```

`SKIP LOCKED` (`lock.timeout = -2`) tells PostgreSQL to skip any row already locked by another transaction rather than blocking. When two pods poll at the same time, each pod's fetch transaction acquires row-level locks; the second pod's query sees those rows as locked and skips them, receiving a non-overlapping subset.

**Important:** This is a best-effort distribution hint, not a hard guarantee. The fetch transaction is short — it commits as soon as the list returns. If both pods' queries run after the first transaction commits, they can both read the same rows. The `@Version` guard and the re-fetch check are the hard defences.

### Per-Event REQUIRES_NEW Transactions

`OutboxEventProcessor.process` is annotated with `@Transactional(propagation = Propagation.REQUIRES_NEW)`:

```java
@Transactional(propagation = Propagation.REQUIRES_NEW)
public void process(OutboxEvent staleRef, MdcContext mdc) { ... }
```

Each event runs in its own independent transaction. A failure processing event N does not roll back checkpoints already committed for events N−1 or N+1 in the same poll batch. The poller loop processes each event sequentially; a failure on one event is caught and logged, and the loop moves on to the next.

### saveAndFlush: Eager Version Checks

After each step, the checkpoint is persisted with `saveAndFlush` rather than `save`:

```java
outboxEventRepository.saveAndFlush(event);
```

`save` queues the SQL UPDATE and only executes it at transaction commit time. If the version conflict is not detected until commit, the resulting `OptimisticLockingFailureException` is thrown outside the method's try/catch blocks, bypasses the error handler, and propagates unexpectedly.

`saveAndFlush` forces the SQL UPDATE to execute immediately — still inside the try/catch. Any `OptimisticLockingFailureException` is raised where the code can handle it deliberately, with a clean rollback of only the current event's transaction.

### @Version: Optimistic Locking

`OutboxEvent` carries a `@Version Long version` field. Hibernate issues:

```sql
UPDATE outbox_event SET current_step=?, payload=?, version=? WHERE id=? AND version=?
```

If two pods both read an event at version 0 and both attempt to save a checkpoint, only the first `UPDATE` matches `WHERE version=0`. PostgreSQL returns 0 rows for the second; Hibernate throws `StaleObjectStateException`; Spring wraps it as `OptimisticLockingFailureException`.

`OutboxEventProcessor` catches this exception specifically and re-throws it so the transaction rolls back cleanly:

```java
} catch (OptimisticLockingFailureException e) {
    log.debug("Event {} had a concurrent update — skipping, another pod owns it", event.getId());
    throw e;  // lets the REQUIRES_NEW tx roll back; poll() catches this outside the tx boundary
}
```

`PipelineOutboxPoller` catches the re-thrown exception at the loop level — outside any transaction boundary — and moves on to the next event:

```java
} catch (OptimisticLockingFailureException e) {
    log.debug("Event {} had concurrent update — skipping, another pod owns it", event.getId());
}
```

**Why re-throw instead of swallow:** Hibernate marks the underlying JDBC connection as rollback-only when it detects the version conflict. If the code catches the exception and returns normally, Spring's transaction interceptor cannot commit and throws `UnexpectedRollbackException` instead. Re-throwing lets the Spring proxy roll back the transaction cleanly, and the original exception propagates to the caller.

### Idempotency Re-fetch Guard

At the start of `process()`, the event is re-fetched inside the new transaction:

```java
OutboxEvent event = outboxEventRepository.findById(staleRef.getId()).orElse(null);
if (event == null || event.getStatus() != OutboxEvent.Status.PENDING) {
    log.debug("Skipping event {} — status={}", ...);
    return;
}
```

The `staleRef` passed into `process()` comes from the poller's batch fetch, which may be seconds old. By the time `process()` runs, another pod may have already completed the event. The re-fetch gets the current DB state. If the status is no longer `PENDING` (it is `PROCESSED` or `FAILED`), the method returns immediately without doing any work.

This guard also handles deleted events and covers the case where `SKIP LOCKED` did not partition the batch.

### Step Checkpointing and Resume

After every step completes, the executor advances `currentStep`, serialises the updated context back to `payload`, and calls `saveAndFlush`. On the last step (or on early exit), `status` is set to `PROCESSED` in the same save to eliminate the brief window where `currentStep == totalSteps` but `status` is still `PENDING`:

```java
boolean isLast = (i == steps.size() - 1) || !continueExecution;
event.setCurrentStep(i + 1);
event.setPayload(objectMapper.writeValueAsString(context));
if (isLast) {
    event.setStatus(OutboxEvent.Status.PROCESSED);
    event.setProcessedAt(LocalDateTime.now());
}
outboxEventRepository.saveAndFlush(event);
```

When a pod crashes mid-pipeline, the next poll fetches the event and re-fetches it inside a fresh transaction. The loop starts from `currentStep` (not from 0), skipping already-completed steps:

```java
for (int i = event.getCurrentStep(); i < steps.size(); i++) {
```

The serialised context carries all outputs from previous steps, so later steps have the data they need (credit score, provider reference, etc.) even when resuming after a restart.

### Retry and Permanent Failure

When a step throws any exception other than `OptimisticLockingFailureException`, `handleFailure` is called:

```java
private void handleFailure(OutboxEvent event, Exception e) {
    int newRetryCount = event.getRetryCount() + 1;
    event.setRetryCount(newRetryCount);
    event.setLastError("Step " + event.getCurrentStep() + ": " + e.getMessage());

    if (newRetryCount >= maxRetries) {
        permanentlyFail(event, e.getMessage());   // status = FAILED, save
    } else {
        outboxEventRepository.save(event);         // status stays PENDING, retryCount++
    }
}
```

The event stays `PENDING` with an incremented `retryCount`. The next poll picks it up again and retries from the same step (the checkpoint was not advanced because the step failed). Once `retryCount >= maxRetries` (default: 3), the event is permanently moved to `FAILED` and will not be retried. The `last_error` column records the failure message for investigation.

Note that the failure path uses `save` (not `saveAndFlush`) because no version conflict is expected — there is nothing to flush eagerly. The transaction commits normally at the end of `process()`.

---

## Observability and Logging

### Structured MDC Fields

Every log line in the pipeline carries four structured fields via SLF4J MDC:

| Field | Key | Set by | Value |
|---|---|---|---|
| Correlation ID | `correlationId` | `MdcContext.forPipelineEvent` | UUID of the outbox event's `correlationId` |
| Pipeline type | `pipelineType` | `MdcContext.forPipelineEvent` | e.g. `CARD_APPLICATION_PIPELINE` |
| Current step | `step` | `OutboxEventProcessor` (per step) | Simple class name of the executing step |
| Request ID | `requestId` | `MdcContext.forHttpRequest` | `X-Request-ID` header, or `gen-<UUID>` if absent |

`MdcContext` is `AutoCloseable` and used in try-with-resources blocks, guaranteeing that MDC keys set for one event are cleared before the next event starts — even if the processing throws an exception:

```java
try (MdcContext mdc = MdcContext.forPipelineEvent(correlationId, pipelineType)) {
    mdc.setStep("CreditBureauCheckStep");
    // correlationId, pipelineType, step appear on every log line within this block
} // all three keys cleared
```

### Log Pattern

`logback-spring.xml` formats every line with all four fields:

```
%d{yyyy-MM-dd HH:mm:ss.SSS} %5level [%thread]
  [cid=%X{correlationId:--}]
  [pipe=%X{pipelineType:--}]
  [step=%X{step:--}]
  [rid=%X{requestId:--}]
  %-50logger{50} : %msg%n
```

A typical pipeline run produces lines like:

```
2026-05-07 21:30:01.123  INFO [scheduling-1] [cid=abc-123] [pipe=CARD_APPLICATION_PIPELINE] [step=-] ... : Pipeline [CARD_APPLICATION_PIPELINE] correlationId=abc-123 → executing step 1/4 (CreditBureauCheckStep)
2026-05-07 21:30:01.456 DEBUG [scheduling-1] [cid=abc-123] [pipe=CARD_APPLICATION_PIPELINE] [step=CreditBureauCheckStep] ... : Calling credit bureau for applicant=Alice Smith
2026-05-07 21:30:01.789  INFO [scheduling-1] [cid=abc-123] [pipe=CARD_APPLICATION_PIPELINE] [step=CreditBureauCheckStep] ... : [Step 0] Credit check complete, score=750
```

The `cid` field makes it possible to grep all log lines for a single application across all pods and all steps:

```bash
grep 'cid=abc-123' /var/log/app.log
```

The `-` default for unset fields (`%X{correlationId:--}`) ensures that log lines outside the pipeline (e.g. HTTP handlers) still parse correctly without breaking log aggregation patterns.

### Async MDC Propagation

When work is dispatched to the async executor (configured in `AsyncConfig`), the MDC context is captured and restored on the target thread via a task decorator:

```java
executor.setTaskDecorator(runnable -> {
    Map<String, String> mdcContext = MDC.getCopyOfContextMap();
    return () -> {
        MDC.setContextMap(mdcContext != null ? mdcContext : Collections.emptyMap());
        try {
            runnable.run();
        } finally {
            MDC.clear();  // never leak into the next task on this pooled thread
        }
    };
});
```

`MdcContext` also provides static `wrap(Runnable)` and `wrap(Supplier<T>)` helpers for `CompletableFuture` usage, capturing the current MDC snapshot at the call site.

---

## Data Model

### `outbox_event`

| Column | Type | Purpose |
|---|---|---|
| `id` | UUID | Primary key |
| `correlation_id` | UUID | Links the event to the business entity |
| `pipeline_type` | VARCHAR | Matches `PipelineStep.getPipelineType()` |
| `current_step` | INTEGER | Index of the next step to execute (0-based); acts as the checkpoint cursor |
| `total_steps` | INTEGER | Number of steps registered for this pipeline at creation time |
| `status` | VARCHAR | `PENDING` → `PROCESSED` or `FAILED` |
| `payload` | TEXT | JSON-serialised pipeline context; updated after every step |
| `retry_count` | INTEGER | Number of failed attempts so far |
| `last_error` | TEXT | Message from the most recent failure |
| `version` | BIGINT | JPA `@Version` column; incremented by every `saveAndFlush` |
| `created_at` | TIMESTAMP | Set at insert |
| `processed_at` | TIMESTAMP | Set when status moves to `PROCESSED` |

A partial index on `(status, created_at) WHERE status = 'PENDING'` ensures the poller's fetch query is fast even with large numbers of completed events.

### `card_application`

The business entity whose status is updated progressively as each pipeline step completes. It mirrors the pipeline progress so application status can be queried cheaply without joining to the outbox table:

```
SUBMITTED → CREDIT_CHECKED → PROVIDER_REGISTERED → NOTIFIED → COMPLETED
```

---

## Configuration Reference

All settings live under the `app` prefix in `application.yml`:

```yaml
app:
  api:
    credit-bureau-url: http://credit-bureau-service  # override per environment
    card-provider-url:  http://card-provider-service
    notification-url:   http://notification-service
  outbox:
    poll-batch-size: 10      # max events fetched per poll cycle
    max-retries:     3       # permanent failure threshold
    poll-delay-ms:   5000    # delay between poll cycles (fixed delay, not rate)
```

`poll-delay-ms` is a **fixed delay** (Spring `@Scheduled(fixedDelayString = ...)`): the next poll starts 5 seconds after the previous one finishes, not 5 seconds after it started. Under heavy load, polls naturally space out without queueing.

---

## Adding a New Pipeline

No changes to the framework classes are needed. Adding a pipeline is three files:

**1. Define a context class:**

```java
@Data @Builder @NoArgsConstructor @AllArgsConstructor
public class LoanApplicationContext {
    private UUID applicationId;
    private String applicantName;
    // ... outputs added by each step
}
```

**2. Implement `PipelineStep<YourContext>` for each step:**

```java
@Component
@RequiredArgsConstructor
public class CreditCheckStep implements PipelineStep<LoanApplicationContext> {

    @Override public String getPipelineType() { return "LOAN_APPLICATION_PIPELINE"; }
    @Override public int getStepIndex()       { return 0; }
    @Override public Class<LoanApplicationContext> getContextClass() {
        return LoanApplicationContext.class;
    }

    @Override
    public boolean execute(LoanApplicationContext ctx) throws Exception {
        // call external service, update ctx, update business entity
        return true; // or false to halt without error
    }
}
```

**3. Publish an outbox event from your service:**

```java
@Transactional
public LoanApplication submitLoan(...) {
    LoanApplication app = loanRepository.save(...);
    outboxService.saveEvent("LOAN_APPLICATION_PIPELINE", app.getId(), context);
    return app;
}
```

`PipelineRegistry` auto-discovers the new step beans at startup and logs the registered pipeline. No changes to `PipelineRegistry`, `OutboxEventProcessor`, or `PipelineOutboxPoller`.

---

## Test Strategy

The test suite is organised in three tiers with different trade-offs between speed, realism, and coverage.

```
95 tests total
├── Tier 1: Unit tests (Mockito)          — fast, no Spring context
├── Tier 2: H2 integration tests           — full Spring context, H2 in-memory DB, WireMock
└── Tier 3: PostgreSQL concurrency tests   — real PostgreSQL via Testcontainers
```

### Tier 1 — Unit Tests (Mockito)

Fast, isolated tests for individual classes with all dependencies mocked. No Spring context, no database.

| Test class | What it covers |
|---|---|
| `OutboxEventProcessorTest` | All execution paths: happy path, checkpoint advancement, resume from checkpoint, step returning false, failure with retry, permanent failure, PENDING guard, deleted-event guard, MDC step updates |
| `PipelineOutboxPollerTest` | Batch fetch delegation, MDC lifecycle, exception isolation between events |
| `PipelineRegistryTest` | Step discovery, ordering by index, single-step warnings, unknown pipeline returns empty |
| `PipelineStepTest` | Each step's API calls, context mutation, entity status updates, and failure behaviour |
| `OutboxServiceTest` | Event creation, pipeline validation, JSON serialisation |
| `CardApplicationServiceTest` | Submit creates entity + event atomically |
| `MdcContextTest` | Key lifecycle, scope isolation, thread-wrapping helpers |
| `ApiClientTest` | HTTP client request/response parsing for all three external APIs |
| `CardApplicationControllerTest` | REST endpoint request/response mapping |
| `AsyncConfigTest` | Async executor configuration |

### Tier 2 — H2 Integration Tests (WireMock)

Full Spring Boot context with an H2 in-memory database (PostgreSQL compatibility mode) and WireMock stubbing the three external APIs. These tests verify behaviour that requires the full application stack but do not need real PostgreSQL semantics.

**`CardApplicationIntegrationTest`** — end-to-end happy and error paths:
- Full 4-step pipeline: submit → poll → COMPLETED
- Atomicity: submit creates both rows; no API call happens until poll
- Retry on transient API failure: event stays PENDING, retryCount increments
- Permanent failure after max retries: event moves to FAILED
- Checkpoint resume: event starts at step > 0, only remaining steps execute

**`OutboxPatternConsistencyIT`** — robustness-focused negative paths (organised in nested test classes):
- `CheckpointDurability`: checkpoint survives pod restart (step N re-runs from N, not from 0); payload updates are durable between steps
- `DataConsistencyUnderFailure`: business entity state reflects last successful step at any point during a partial run
- `TerminalStateImmutability`: PROCESSED and FAILED events are not re-processed on subsequent polls
- `RetryAndRecovery`: retryCount increments correctly; recovery after transient failure works
- `ErrorTypeCoverage`: null response, unexpected status, serialisation failure, repository exception
- `BatchProcessingIsolation`: failure on one event does not affect others in the same batch

### Tier 3 — PostgreSQL Concurrency Tests (Testcontainers)

Tests that require a real PostgreSQL database because H2 does not honour `SKIP LOCKED` semantics and does not reproduce PostgreSQL's MVCC lock contention. A single `postgres:16-alpine` container is shared across all three tests via the singleton container pattern.

**`ConcurrentOutboxIT`** — three targeted concurrency tests:

**Test 1: `@Version` optimistic lock conflict**

```
Pod A: save entity (version=0 → 1)
Pod B: load stale copy (version=0), try to save → OptimisticLockingFailureException
```

Proves that `@Version` prevents a stale write from silently overwriting a concurrent commit. This is the hard guarantee that prevents duplicate pipeline progress from being committed.

**Test 2: `SKIP LOCKED` pod distribution**

```
Pod 1: SELECT FOR UPDATE → acquires locks on 4 rows, holds tx open via CountDownLatch
Pod 2: SELECT FOR UPDATE SKIP LOCKED → receives 0 rows (all locked by Pod 1)
```

Proves that when two pods' fetch transactions overlap, `SKIP LOCKED` returns a non-overlapping result set. Pod 2 gets zero rows and does not compete with Pod 1 on those events. This is the soft distribution mechanism.

**Test 3: Two-pod end-to-end idempotency**

```
4 events created → 2 pods released simultaneously → all 4 events PROCESSED
```

Proves the full concurrency model holds end-to-end:

1. All 4 events reach `PROCESSED` status with no events missed or stuck.
2. All 4 `CardApplication` records reach `COMPLETED`.
3. External APIs called at least 4 times each — proves no event was silently skipped. Duplicate calls are permitted: when both pods race on the same event, both may call the external API, but only one pod's checkpoint commits (via `@Version`). Pipeline steps must therefore be idempotent.
4. A second `poller.poll()` after all events are `PROCESSED` calls zero external APIs — proves the re-fetch idempotency guard prevents redundant re-processing.

#### Test infrastructure

`PostgresBaseTest` is an abstract base class that provides the Testcontainers container and WireMock server to all PostgreSQL tests:

```java
@SpringBootTest(webEnvironment = RANDOM_PORT)
@ActiveProfiles("postgres-test")
@Testcontainers
public abstract class PostgresBaseTest {

    @Container
    @ServiceConnection  // auto-wires spring.datasource.* from the container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");
    ...
}
```

`@ServiceConnection` (Spring Boot 3.2) eliminates the need for a `@DynamicPropertySource` to configure the datasource. The `postgres-test` profile (`application-postgres-test.yml`) sets `spring.jpa.database-platform` to `PostgreSQLDialect` and points Flyway at the production migration SQL (`filesystem:src/main/resources/db/migration`) so the real schema runs against the real database.

`WireMockBaseTest` is a separate base class for the H2 integration tests. `PostgresBaseTest` deliberately does not extend it — `WireMockBaseTest` hard-codes `@ActiveProfiles("test")` which would activate the H2 datasource config.

---

## Running the Tests

### Prerequisites

- Java 17+
- Docker Desktop (for Tier 3 PostgreSQL tests)
- Docker Desktop 4.36+ / Engine 27+ requires the `DOCKER_API_VERSION=1.44` env var (configured automatically in `pom.xml`)

### Run all tests

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home mvn clean test
```

### Run only unit tests

```bash
mvn test -Dtest="*Test" -Dsurefire.failIfNoSpecifiedTests=false
```

### Run only integration tests (H2)

```bash
mvn test -Dtest="CardApplicationIntegrationTest,OutboxPatternConsistencyIT" -Dsurefire.failIfNoSpecifiedTests=false
```

### Run only PostgreSQL concurrency tests

```bash
mvn test -Dtest="ConcurrentOutboxIT" -Dsurefire.failIfNoSpecifiedTests=false
```

### JaCoCo coverage report

Generated automatically during `mvn test` at `target/site/jacoco/index.html`. `OutboxDemoApplication` is excluded from coverage (Spring Boot entry point with no testable logic).

---

## Running Locally

### Prerequisites

- Java 17+
- Docker (for PostgreSQL)

### Start PostgreSQL

```bash
docker run -d \
  --name outbox-demo-pg \
  -e POSTGRES_DB=outbox_demo \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  postgres:16-alpine
```

### Start the application

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home \
  mvn spring-boot:run
```

Flyway runs migrations automatically on startup. The application starts on port 8080.

### Submit a card application

```bash
curl -s -X POST http://localhost:8080/applications \
  -H 'Content-Type: application/json' \
  -d '{"applicantName":"Alice Smith","email":"alice@example.com","annualIncome":85000}' \
  | jq .
```

The response returns immediately with `status: SUBMITTED`. The outbox poller runs every 5 seconds and drives the pipeline. Poll the status endpoint to observe progress:

```bash
curl -s http://localhost:8080/applications/{id} | jq .status
```

Without real downstream services running, pipeline steps will fail and retry. Configure `app.api.*` properties to point at real or mock services.
