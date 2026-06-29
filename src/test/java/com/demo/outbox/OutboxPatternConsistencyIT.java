package com.demo.outbox;

import com.demo.outbox.entity.CardApplication;
import com.demo.outbox.entity.OutboxEvent;
import com.demo.outbox.repository.CardApplicationRepository;
import com.demo.outbox.repository.OutboxEventRepository;
import com.demo.outbox.scheduler.PipelineOutboxPoller;
import com.demo.outbox.service.CardApplicationService;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;

import java.math.BigDecimal;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.*;

/**
 * Robustness-focused integration tests for the Transactional Outbox Pattern.
 *
 * Uses a real Spring context, H2 (PostgreSQL mode), and WireMock.
 * Emphasis is on negative paths: retries, checkpoint durability, terminal state
 * immutability, partial-pipeline consistency, and error type coverage.
 */
@DisplayName("Outbox Pattern — Data Consistency Integration Tests")
class OutboxPatternConsistencyIT extends WireMockBaseTest {

    @Autowired CardApplicationService cardApplicationService;
    @Autowired PipelineOutboxPoller poller;
    @Autowired CardApplicationRepository cardApplicationRepository;
    @Autowired OutboxEventRepository outboxEventRepository;

    @BeforeEach
    void cleanDb() {
        outboxEventRepository.deleteAll();
        cardApplicationRepository.deleteAll();
    }

    // ─── Happy path ───────────────────────────────────────────────────────────

    @Test
    @DisplayName("Happy path: submit returns instantly with SUBMITTED status; pipeline drives to COMPLETED")
    void happyPath_atomicSubmitAndFullPipelineCompletion() {
        stubAllApisSuccess();

        CardApplication app = cardApplicationService.submitApplication(
                "Alice Smith", "alice@example.com", new BigDecimal("85000"));

        // Immediately after submit: entity is SUBMITTED, outbox is PENDING, zero API calls
        assertThat(app.getStatus()).isEqualTo(CardApplication.Status.SUBMITTED);
        assertThat(outboxEventRepository.count()).isEqualTo(1);
        wireMock.verify(0, anyRequestedFor(anyUrl()));

        poller.poll();

        CardApplication completed = cardApplicationRepository.findById(app.getId()).orElseThrow();
        assertThat(completed.getStatus()).isEqualTo(CardApplication.Status.COMPLETED);
        assertThat(completed.getCreditScore()).isNotNull();
        assertThat(completed.getProviderRef()).isNotNull();
        assertThat(completed.getNotificationId()).isNotNull();

        OutboxEvent event = outboxEventRepository.findAll().get(0);
        assertThat(event.getStatus()).isEqualTo(OutboxEvent.Status.PROCESSED);
        assertThat(event.getProcessedAt()).isNotNull();
        assertThat(event.getRetryCount()).isZero();
        assertThat(event.getCurrentStep()).isEqualTo(4); // all steps passed
    }

    // ─── Negative: retry and recovery ────────────────────────────────────────

    @Nested
    @DisplayName("Retry and recovery")
    class RetryAndRecovery {

        @Test
        @DisplayName("Intermittent failure: step 0 fails twice then recovers — application reaches COMPLETED")
        void intermittentCreditBureauFailure_recoversOnThirdAttempt() {
            // WireMock scenario: fail twice, succeed on third attempt
            wireMock.stubFor(post(urlEqualTo("/credit/check"))
                    .inScenario("credit-flaky")
                    .whenScenarioStateIs(Scenario.STARTED)
                    .willReturn(serverError())
                    .willSetStateTo("SECOND_ATTEMPT"));

            wireMock.stubFor(post(urlEqualTo("/credit/check"))
                    .inScenario("credit-flaky")
                    .whenScenarioStateIs("SECOND_ATTEMPT")
                    .willReturn(serverError())
                    .willSetStateTo("THIRD_ATTEMPT"));

            wireMock.stubFor(post(urlEqualTo("/credit/check"))
                    .inScenario("credit-flaky")
                    .whenScenarioStateIs("THIRD_ATTEMPT")
                    .willReturn(okJson("{\"creditScore\": 720}")));

            stubCardProvider("PROV-FLAKY-001");
            stubNotification("NOTIF-FLAKY-001");

            cardApplicationService.submitApplication(
                    "Eve Flaky", "eve@example.com", new BigDecimal("65000"));

            // Poll 1 — step 0 fails
            poller.poll();
            OutboxEvent after1 = outboxEventRepository.findAll().get(0);
            assertThat(after1.getStatus()).isEqualTo(OutboxEvent.Status.PENDING);
            assertThat(after1.getRetryCount()).isEqualTo(1);
            assertThat(after1.getCurrentStep()).isZero(); // not advanced past failing step

            // Poll 2 — step 0 fails again
            poller.poll();
            OutboxEvent after2 = outboxEventRepository.findAll().get(0);
            assertThat(after2.getStatus()).isEqualTo(OutboxEvent.Status.PENDING);
            assertThat(after2.getRetryCount()).isEqualTo(2);

            // Poll 3 — step 0 succeeds, entire pipeline completes
            poller.poll();
            OutboxEvent after3 = outboxEventRepository.findAll().get(0);
            assertThat(after3.getStatus()).isEqualTo(OutboxEvent.Status.PROCESSED);

            CardApplication finalApp = cardApplicationRepository.findAll().get(0);
            assertThat(finalApp.getStatus()).isEqualTo(CardApplication.Status.COMPLETED);
            assertThat(finalApp.getCreditScore()).isEqualTo(720);

            // Credit bureau was called exactly 3 times — no more, no less
            wireMock.verify(3, postRequestedFor(urlEqualTo("/credit/check")));
        }

        @Test
        @DisplayName("maxRetries boundary: event becomes FAILED on exactly the Nth failure, not before")
        void maxRetries_eventFailsOnExactlyNthAttempt() {
            stubCreditBureauFailure();

            cardApplicationService.submitApplication(
                    "Frank Boundary", "frank@example.com", new BigDecimal("40000"));

            // maxRetries = 3; two polls should still leave the event PENDING
            poller.poll();
            assertThat(outboxEventRepository.findAll().get(0).getStatus())
                    .isEqualTo(OutboxEvent.Status.PENDING);

            poller.poll();
            assertThat(outboxEventRepository.findAll().get(0).getStatus())
                    .isEqualTo(OutboxEvent.Status.PENDING);

            // Third poll crosses the maxRetries threshold → FAILED
            poller.poll();
            OutboxEvent failed = outboxEventRepository.findAll().get(0);
            assertThat(failed.getStatus()).isEqualTo(OutboxEvent.Status.FAILED);
            assertThat(failed.getRetryCount()).isEqualTo(3);
            assertThat(failed.getLastError()).isNotBlank();
        }
    }

    // ─── Negative: checkpoint durability ─────────────────────────────────────

    @Nested
    @DisplayName("Checkpoint durability — successful steps are never replayed")
    class CheckpointDurability {

        @Test
        @DisplayName("Step 1 failure: credit bureau called exactly once across all retry attempts")
        void step1Failure_creditBureauNotCalledOnRetry() {
            stubCreditBureau(680);
            stubCardProviderFailure();

            cardApplicationService.submitApplication(
                    "Grace Retry", "grace@example.com", new BigDecimal("72000"));

            // Poll 1: step 0 succeeds → checkpoint at currentStep=1; step 1 fails
            poller.poll();

            OutboxEvent midPoll = outboxEventRepository.findAll().get(0);
            assertThat(midPoll.getCurrentStep()).isEqualTo(1);
            assertThat(midPoll.getStatus()).isEqualTo(OutboxEvent.Status.PENDING);
            wireMock.verify(1, postRequestedFor(urlEqualTo("/credit/check")));

            // Fix step 1, provide step 2 stub, retry
            stubCardProvider("PROV-GRACE-001");
            stubNotification("NOTIF-GRACE-001");

            // Poll 2: resumes at currentStep=1 — credit bureau is NOT called again
            poller.poll();

            wireMock.verify(1, postRequestedFor(urlEqualTo("/credit/check"))); // still exactly 1

            CardApplication finalApp = cardApplicationRepository.findAll().get(0);
            assertThat(finalApp.getStatus()).isEqualTo(CardApplication.Status.COMPLETED);
            assertThat(finalApp.getCreditScore()).isEqualTo(680); // original score preserved in context
        }

        @Test
        @DisplayName("Step 2 failure: only notification is retried — credit bureau and card provider untouched")
        void step2Failure_onlyNotificationReplayed() {
            stubCreditBureau(710);
            stubCardProvider("PROV-STEP2-001");
            stubNotificationFailure();

            cardApplicationService.submitApplication(
                    "Henry Notif", "henry@example.com", new BigDecimal("80000"));

            // Poll 1: steps 0 and 1 succeed → checkpoint at currentStep=2; step 2 fails
            poller.poll();

            OutboxEvent event = outboxEventRepository.findAll().get(0);
            assertThat(event.getCurrentStep()).isEqualTo(2); // two steps checkpointed
            assertThat(event.getStatus()).isEqualTo(OutboxEvent.Status.PENDING);
            assertThat(event.getRetryCount()).isEqualTo(1);

            // Steps 0 and 1 each called once
            wireMock.verify(1, postRequestedFor(urlEqualTo("/credit/check")));
            wireMock.verify(1, postRequestedFor(urlEqualTo("/cards/register")));

            // Fix step 2 and retry
            stubNotification("NOTIF-RETRY-002");
            poller.poll();

            // Steps 0 and 1 were NOT called again
            wireMock.verify(1, postRequestedFor(urlEqualTo("/credit/check")));
            wireMock.verify(1, postRequestedFor(urlEqualTo("/cards/register")));
            // Notification was called twice (first attempt failed, second succeeded)
            wireMock.verify(2, postRequestedFor(urlEqualTo("/notifications/send")));

            CardApplication finalApp = cardApplicationRepository.findAll().get(0);
            assertThat(finalApp.getStatus()).isEqualTo(CardApplication.Status.COMPLETED);
            assertThat(finalApp.getNotificationId()).isEqualTo("NOTIF-RETRY-002");
        }

        @Test
        @DisplayName("Context data survives failure and checkpoint: credit score available on retry without re-calling bureau")
        void contextPayload_preservedAcrossRetry() {
            stubCreditBureau(695);
            stubCardProviderFailure(); // step 1 will fail on first poll

            cardApplicationService.submitApplication(
                    "Iris Context", "iris@example.com", new BigDecimal("60000"));

            poller.poll(); // step 0 ok (score=695 stored in payload), step 1 fails

            // Verify the credit score was checkpointed into the payload
            OutboxEvent event = outboxEventRepository.findAll().get(0);
            assertThat(event.getPayload()).contains("695"); // creditScore in JSON

            // Now fix step 1 — credit bureau must not be called again
            stubCardProvider("PROV-IRIS-001");
            stubNotification("NOTIF-IRIS-001");
            poller.poll();

            // The entity must carry the score from the first poll's context, not a fresh call
            CardApplication app = cardApplicationRepository.findAll().get(0);
            assertThat(app.getCreditScore()).isEqualTo(695);
            assertThat(app.getStatus()).isEqualTo(CardApplication.Status.COMPLETED);
            wireMock.verify(1, postRequestedFor(urlEqualTo("/credit/check")));
        }
    }

    // ─── Negative: terminal state immutability ────────────────────────────────

    @Nested
    @DisplayName("Terminal state immutability — PROCESSED and FAILED events are never re-executed")
    class TerminalStateImmutability {

        @Test
        @DisplayName("PROCESSED event: subsequent polls make zero API calls and leave status unchanged")
        void processedEvent_isIdempotentUnderRepeatedPolls() {
            stubAllApisSuccess();

            cardApplicationService.submitApplication(
                    "Ida Processed", "ida@example.com", new BigDecimal("55000"));

            poller.poll(); // pipeline completes, event becomes PROCESSED

            assertThat(outboxEventRepository.findAll().get(0).getStatus())
                    .isEqualTo(OutboxEvent.Status.PROCESSED);

            // Reset WireMock call history, then poll several more times
            wireMock.resetRequests();
            poller.poll();
            poller.poll();
            poller.poll();

            // Absolutely zero API calls after the event was PROCESSED
            wireMock.verify(0, anyRequestedFor(anyUrl()));

            // Entity still COMPLETED, not re-written or corrupted
            CardApplication app = cardApplicationRepository.findAll().get(0);
            assertThat(app.getStatus()).isEqualTo(CardApplication.Status.COMPLETED);
        }

        @Test
        @DisplayName("FAILED event: subsequent polls leave retryCount and status completely unchanged")
        void failedEvent_isPermanentlyIgnoredByPoller() {
            stubCreditBureauFailure();

            cardApplicationService.submitApplication(
                    "Jack Failed", "jack@example.com", new BigDecimal("30000"));

            // Exhaust retries
            poller.poll();
            poller.poll();
            poller.poll(); // retryCount = 3 → FAILED

            OutboxEvent failedSnapshot = outboxEventRepository.findAll().get(0);
            assertThat(failedSnapshot.getStatus()).isEqualTo(OutboxEvent.Status.FAILED);
            int lockedRetryCount = failedSnapshot.getRetryCount();

            // Poll again multiple times — FAILED is a dead-letter, poller must not touch it
            wireMock.resetRequests();
            poller.poll();
            poller.poll();

            OutboxEvent unchanged = outboxEventRepository.findAll().get(0);
            assertThat(unchanged.getRetryCount()).isEqualTo(lockedRetryCount);
            assertThat(unchanged.getStatus()).isEqualTo(OutboxEvent.Status.FAILED);

            // No further API calls after FAILED
            wireMock.verify(0, anyRequestedFor(anyUrl()));
        }

        @Test
        @DisplayName("FAILED event: retryCount is exactly maxRetries, not maxRetries+1")
        void failedEvent_retryCountDoesNotExceedMaxRetries() {
            stubCreditBureauFailure();

            cardApplicationService.submitApplication(
                    "Kim Count", "kim@example.com", new BigDecimal("45000"));

            // Poll maxRetries+5 times — retryCount must cap at maxRetries
            for (int i = 0; i < 8; i++) {
                poller.poll();
            }

            OutboxEvent event = outboxEventRepository.findAll().get(0);
            assertThat(event.getStatus()).isEqualTo(OutboxEvent.Status.FAILED);
            assertThat(event.getRetryCount()).isEqualTo(3); // maxRetries = 3, never higher
        }
    }

    // ─── Negative: data consistency under permanent failure ───────────────────

    @Nested
    @DisplayName("Data consistency under permanent failure")
    class DataConsistencyUnderFailure {

        @Test
        @DisplayName("Permanent failure mid-pipeline: entity status reflects last SUCCESSFUL step, not COMPLETED")
        void permanentFailure_entityStatusIsLastSuccessfulStep() {
            stubCreditBureau(690);
            stubCardProviderFailure(); // step 1 always fails → permanent failure

            cardApplicationService.submitApplication(
                    "Karen Partial", "karen@example.com", new BigDecimal("68000"));

            // Three polls exhaust retries; step 0 succeeds on first poll, step 1 never does
            poller.poll(); // step 0 ok, step 1 fails (retryCount=1)
            poller.poll(); // step 1 fails again (retryCount=2)
            poller.poll(); // step 1 fails (retryCount=3 → FAILED)

            OutboxEvent failed = outboxEventRepository.findAll().get(0);
            assertThat(failed.getStatus()).isEqualTo(OutboxEvent.Status.FAILED);

            // Entity must reflect what DID succeed: CREDIT_CHECKED (step 0 done)
            CardApplication app = cardApplicationRepository.findAll().get(0);
            assertThat(app.getStatus()).isEqualTo(CardApplication.Status.CREDIT_CHECKED);
            assertThat(app.getCreditScore()).isEqualTo(690);  // step 0 data present
            assertThat(app.getProviderRef()).isNull();         // step 1 never succeeded
            assertThat(app.getNotificationId()).isNull();      // step 2 never ran
        }

        @Test
        @DisplayName("Permanent failure at step 0: entity stays SUBMITTED — no partial data written")
        void permanentFailureAtStep0_entityRemainsSubmitted() {
            stubCreditBureauFailure(); // all polls fail at step 0

            cardApplicationService.submitApplication(
                    "Leo Zero", "leo@example.com", new BigDecimal("52000"));

            poller.poll();
            poller.poll();
            poller.poll();

            CardApplication app = cardApplicationRepository.findAll().get(0);
            assertThat(app.getStatus()).isEqualTo(CardApplication.Status.SUBMITTED);
            assertThat(app.getCreditScore()).isNull();
            assertThat(app.getProviderRef()).isNull();
            assertThat(app.getNotificationId()).isNull();
        }

        @Test
        @DisplayName("Atomicity: submit creates card_application and outbox_event together — correlation is correct")
        void submit_bothRecordsCreatedAtomically_correlationLinked() {
            CardApplication app = cardApplicationService.submitApplication(
                    "Mia Atomic", "mia@example.com", new BigDecimal("50000"));

            assertThat(cardApplicationRepository.count()).isEqualTo(1);
            assertThat(outboxEventRepository.count()).isEqualTo(1);

            OutboxEvent event = outboxEventRepository.findAll().get(0);
            // The outbox event must point back to the exact card_application row
            assertThat(event.getCorrelationId()).isEqualTo(app.getId());
            assertThat(event.getStatus()).isEqualTo(OutboxEvent.Status.PENDING);
            assertThat(event.getCurrentStep()).isZero();
            assertThat(event.getPayload()).contains(app.getId().toString());
        }
    }

    // ─── Negative: error type coverage ───────────────────────────────────────

    @Nested
    @DisplayName("All error types trigger the retry mechanism")
    class ErrorTypeCoverage {

        @Test
        @DisplayName("HTTP 500 (server error): increments retryCount, leaves event PENDING")
        void serverError500_incrementsRetryCount() {
            stubCreditBureauFailure(); // 500

            cardApplicationService.submitApplication(
                    "Nina 500", "nina@example.com", new BigDecimal("58000"));
            poller.poll();

            OutboxEvent event = outboxEventRepository.findAll().get(0);
            assertThat(event.getStatus()).isEqualTo(OutboxEvent.Status.PENDING);
            assertThat(event.getRetryCount()).isEqualTo(1);
            assertThat(event.getLastError()).isNotBlank();
        }

        @Test
        @DisplayName("HTTP 4xx (client error): also treated as retryable failure")
        void clientError4xx_isAlsoRetried() {
            wireMock.stubFor(post(urlEqualTo("/credit/check"))
                    .willReturn(badRequest())); // 400

            cardApplicationService.submitApplication(
                    "Oscar 4xx", "oscar@example.com", new BigDecimal("47000"));
            poller.poll();

            OutboxEvent event = outboxEventRepository.findAll().get(0);
            assertThat(event.getStatus()).isEqualTo(OutboxEvent.Status.PENDING);
            assertThat(event.getRetryCount()).isEqualTo(1);
        }

        @Test
        @DisplayName("Malformed API response (200 OK but missing expected field): treated as application-level error")
        void malformedApiResponse_treatedAsRetryableError() {
            // HTTP 200 but the 'creditScore' field is absent — client throws IllegalStateException
            wireMock.stubFor(post(urlEqualTo("/credit/check"))
                    .willReturn(okJson("{\"unexpectedField\": \"surprise\"}")));

            cardApplicationService.submitApplication(
                    "Paula Malformed", "paula@example.com", new BigDecimal("61000"));
            poller.poll();

            OutboxEvent event = outboxEventRepository.findAll().get(0);
            assertThat(event.getStatus()).isEqualTo(OutboxEvent.Status.PENDING);
            assertThat(event.getRetryCount()).isEqualTo(1);
            assertThat(event.getLastError()).contains("Step 0");
        }

        @Test
        @DisplayName("Malformed API response: eventually fails permanently after maxRetries")
        void malformedApiResponse_eventuallyFails() {
            wireMock.stubFor(post(urlEqualTo("/credit/check"))
                    .willReturn(okJson("{\"unexpectedField\": \"surprise\"}")));

            cardApplicationService.submitApplication(
                    "Quinn BadJson", "quinn@example.com", new BigDecimal("53000"));

            poller.poll();
            poller.poll();
            poller.poll();

            assertThat(outboxEventRepository.findAll().get(0).getStatus())
                    .isEqualTo(OutboxEvent.Status.FAILED);
        }

        @Test
        @DisplayName("Notification service returns malformed response: card provider not called again on retry")
        void notificationMalformed_onlyNotificationRetried() {
            stubCreditBureau(730);
            stubCardProvider("PROV-NOTIF-MALFORMED-001");
            // Notification returns 200 but empty body — missing notificationId field
            wireMock.stubFor(post(urlEqualTo("/notifications/send"))
                    .willReturn(okJson("{}")));

            cardApplicationService.submitApplication(
                    "Rita MalNotif", "rita@example.com", new BigDecimal("76000"));

            poller.poll(); // steps 0 and 1 succeed, step 2 gets malformed response

            OutboxEvent event = outboxEventRepository.findAll().get(0);
            assertThat(event.getCurrentStep()).isEqualTo(2); // checkpointed after steps 0 and 1
            assertThat(event.getStatus()).isEqualTo(OutboxEvent.Status.PENDING);

            wireMock.verify(1, postRequestedFor(urlEqualTo("/credit/check")));
            wireMock.verify(1, postRequestedFor(urlEqualTo("/cards/register")));

            // Fix notification and retry — only notification is called again
            stubNotification("NOTIF-FIXED-001");
            poller.poll();

            wireMock.verify(1, postRequestedFor(urlEqualTo("/credit/check")));
            wireMock.verify(1, postRequestedFor(urlEqualTo("/cards/register")));
            wireMock.verify(2, postRequestedFor(urlEqualTo("/notifications/send")));

            assertThat(cardApplicationRepository.findAll().get(0).getStatus())
                    .isEqualTo(CardApplication.Status.COMPLETED);
        }
    }

    // ─── Negative: batch processing isolation ────────────────────────────────

    @Nested
    @DisplayName("Batch processing isolation — one failure must not affect sibling applications")
    class BatchProcessingIsolation {

        @Test
        @DisplayName("Mixed batch: one application fails, others in the same batch still complete")
        void mixedBatch_failureDoesNotBlockSiblings() {
            // First application: credit bureau will fail (first call → 500)
            // Second application: credit bureau succeeds (second call onwards)
            wireMock.stubFor(post(urlEqualTo("/credit/check"))
                    .inScenario("mixed-batch")
                    .whenScenarioStateIs(Scenario.STARTED)
                    .willReturn(serverError())
                    .willSetStateTo("SUCCESS_MODE"));

            wireMock.stubFor(post(urlEqualTo("/credit/check"))
                    .inScenario("mixed-batch")
                    .whenScenarioStateIs("SUCCESS_MODE")
                    .willReturn(okJson("{\"creditScore\": 760}")));

            stubCardProvider("PROV-MIXED-001");
            stubNotification("NOTIF-MIXED-001");

            // Submit both — first submitted will be processed first (order by created_at)
            CardApplication failingApp = cardApplicationService.submitApplication(
                    "Sam Fail", "sam@example.com", new BigDecimal("35000"));
            CardApplication succeedingApp = cardApplicationService.submitApplication(
                    "Tina Win", "tina@example.com", new BigDecimal("80000"));

            // Single poll processes both: failingApp gets 500, succeedingApp gets 760 and completes
            poller.poll();

            CardApplication failingState = cardApplicationRepository
                    .findById(failingApp.getId()).orElseThrow();
            CardApplication succeedingState = cardApplicationRepository
                    .findById(succeedingApp.getId()).orElseThrow();

            assertThat(failingState.getStatus()).isEqualTo(CardApplication.Status.SUBMITTED);
            assertThat(succeedingState.getStatus()).isEqualTo(CardApplication.Status.COMPLETED);

            OutboxEvent failingEvent = outboxEventRepository.findAll().stream()
                    .filter(e -> e.getCorrelationId().equals(failingApp.getId()))
                    .findFirst().orElseThrow();
            OutboxEvent succeedingEvent = outboxEventRepository.findAll().stream()
                    .filter(e -> e.getCorrelationId().equals(succeedingApp.getId()))
                    .findFirst().orElseThrow();

            assertThat(failingEvent.getStatus()).isEqualTo(OutboxEvent.Status.PENDING);
            assertThat(failingEvent.getRetryCount()).isEqualTo(1);
            assertThat(succeedingEvent.getStatus()).isEqualTo(OutboxEvent.Status.PROCESSED);
        }

        @Test
        @DisplayName("Mixed batch: FAILED event does not prevent fresh applications from being processed")
        void failedEvent_doesNotBlockNewApplications() {
            stubCreditBureauFailure();

            // Create one application and exhaust its retries → FAILED
            cardApplicationService.submitApplication(
                    "Uma Stuck", "uma@example.com", new BigDecimal("33000"));
            poller.poll();
            poller.poll();
            poller.poll();

            assertThat(outboxEventRepository.findAll().get(0).getStatus())
                    .isEqualTo(OutboxEvent.Status.FAILED);

            // Now stub success and submit a fresh application
            stubAllApisSuccess();
            CardApplication freshApp = cardApplicationService.submitApplication(
                    "Victor Fresh", "victor@example.com", new BigDecimal("90000"));

            // One poll should process only the fresh PENDING event
            poller.poll();

            CardApplication freshState = cardApplicationRepository
                    .findById(freshApp.getId()).orElseThrow();
            assertThat(freshState.getStatus()).isEqualTo(CardApplication.Status.COMPLETED);

            // The originally FAILED event must remain untouched
            long failedCount = outboxEventRepository.findAll().stream()
                    .filter(e -> e.getStatus() == OutboxEvent.Status.FAILED)
                    .count();
            assertThat(failedCount).isEqualTo(1);
        }

        @Test
        @DisplayName("Three concurrent applications all succeed in a single poll batch")
        void threeConcurrentApplications_allCompleteInOnePoll() {
            stubAllApisSuccess();

            cardApplicationService.submitApplication("User One",   "u1@example.com", new BigDecimal("50000"));
            cardApplicationService.submitApplication("User Two",   "u2@example.com", new BigDecimal("60000"));
            cardApplicationService.submitApplication("User Three", "u3@example.com", new BigDecimal("70000"));

            assertThat(outboxEventRepository.count()).isEqualTo(3);

            poller.poll();

            long completedApps = cardApplicationRepository.findAll().stream()
                    .filter(a -> a.getStatus() == CardApplication.Status.COMPLETED)
                    .count();
            long processedEvents = outboxEventRepository.findAll().stream()
                    .filter(e -> e.getStatus() == OutboxEvent.Status.PROCESSED)
                    .count();

            assertThat(completedApps).isEqualTo(3);
            assertThat(processedEvents).isEqualTo(3);
        }
    }
}
