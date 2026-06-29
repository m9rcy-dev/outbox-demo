package com.demo.outbox;

import com.demo.outbox.entity.CardApplication;
import com.demo.outbox.entity.OutboxEvent;
import com.demo.outbox.repository.CardApplicationRepository;
import com.demo.outbox.repository.OutboxEventRepository;
import com.demo.outbox.scheduler.PipelineOutboxPoller;
import com.demo.outbox.service.CardApplicationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Full integration tests.
 *
 * Real Spring context + real H2 database + WireMock for external APIs.
 * Tests the complete flow from HTTP submission through pipeline execution.
 */
@DisplayName("Full pipeline integration tests")
class CardApplicationIntegrationTest extends WireMockBaseTest {

    @Autowired CardApplicationService cardApplicationService;
    @Autowired PipelineOutboxPoller poller;
    @Autowired CardApplicationRepository cardApplicationRepository;
    @Autowired OutboxEventRepository outboxEventRepository;

    @BeforeEach
    void cleanDb() {
        outboxEventRepository.deleteAll();
        cardApplicationRepository.deleteAll();
    }

    // ── Happy path ────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Happy path: all 4 steps execute, application reaches COMPLETED")
    void happyPath_allStepsExecute_applicationCompleted() {
        stubAllApisSuccess();

        // Submit — only writes to DB, no API calls yet
        CardApplication submitted = cardApplicationService.submitApplication(
            "Alice Smith", "alice@example.com", new BigDecimal("85000")
        );

        assertThat(submitted.getStatus()).isEqualTo(CardApplication.Status.SUBMITTED);

        // Outbox event created
        List<OutboxEvent> events = outboxEventRepository.findAll();
        assertThat(events).hasSize(1);
        assertThat(events.get(0).getStatus()).isEqualTo(OutboxEvent.Status.PENDING);
        assertThat(events.get(0).getCurrentStep()).isZero();

        // Run the poller — drives all 4 steps
        poller.poll();

        // Verify final entity state
        CardApplication finalApp = cardApplicationRepository.findById(submitted.getId()).orElseThrow();
        assertThat(finalApp.getStatus()).isEqualTo(CardApplication.Status.COMPLETED);
        assertThat(finalApp.getCreditScore()).isEqualTo(750);
        assertThat(finalApp.getProviderRef()).isEqualTo("PROV-REF-001");
        assertThat(finalApp.getNotificationId()).isEqualTo("NOTIF-001");

        // Outbox event marked processed
        OutboxEvent event = outboxEventRepository.findAll().get(0);
        assertThat(event.getStatus()).isEqualTo(OutboxEvent.Status.PROCESSED);
        assertThat(event.getProcessedAt()).isNotNull();
        assertThat(event.getCurrentStep()).isEqualTo(4); // all steps done
    }

    // ── Atomicity ─────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Atomicity: outbox event exists even before poller runs (no API calls on submit)")
    void submit_noApiCallsMade_outboxRowPresent() {
        // Do NOT stub APIs — if any API were called during submit it would throw

        CardApplication app = cardApplicationService.submitApplication(
            "Bob Jones", "bob@example.com", new BigDecimal("60000")
        );

        assertThat(app.getStatus()).isEqualTo(CardApplication.Status.SUBMITTED);
        assertThat(outboxEventRepository.count()).isEqualTo(1);

        // WireMock was never hit
        wireMock.verify(0, com.github.tomakehurst.wiremock.client.WireMock.anyRequestedFor(
            com.github.tomakehurst.wiremock.matching.UrlPattern.ANY));
    }

    // ── Step-by-step status progression ──────────────────────────────────────

    @Test
    @DisplayName("Status progression: entity status advances with each step")
    void statusProgression_entityStatusAdvancesWithEachStep() {
        stubAllApisSuccess();

        CardApplication app = cardApplicationService.submitApplication(
            "Carol White", "carol@example.com", new BigDecimal("70000")
        );

        // After submit
        assertThat(cardApplicationRepository.findById(app.getId()).orElseThrow().getStatus())
            .isEqualTo(CardApplication.Status.SUBMITTED);

        // After full pipeline
        poller.poll();

        CardApplication finalApp = cardApplicationRepository.findById(app.getId()).orElseThrow();
        assertThat(finalApp.getStatus()).isEqualTo(CardApplication.Status.COMPLETED);
    }

    // ── Failure and retry ─────────────────────────────────────────────────────

    @Test
    @DisplayName("Retry: step 0 fails, event stays PENDING with retryCount=1")
    void step0Fails_eventStaysPendingWithIncrementedRetryCount() {
        stubCreditBureauFailure();

        cardApplicationService.submitApplication(
            "Dan Brown", "dan@example.com", new BigDecimal("55000")
        );

        poller.poll();

        OutboxEvent event = outboxEventRepository.findAll().get(0);
        assertThat(event.getStatus()).isEqualTo(OutboxEvent.Status.PENDING);
        assertThat(event.getRetryCount()).isEqualTo(1);
        assertThat(event.getCurrentStep()).isZero(); // not checkpointed past step 0
        assertThat(event.getLastError()).isNotBlank();
    }

    @Test
    @DisplayName("Retry: step 1 fails, step 0 is NOT re-executed on retry (checkpoint)")
    void step1Fails_step0NotReExecutedOnRetry() {
        stubCreditBureau(700);
        stubCardProviderFailure();

        cardApplicationService.submitApplication(
            "Eve Davis", "eve@example.com", new BigDecimal("75000")
        );

        // First poll — step 0 succeeds, step 1 fails
        poller.poll();

        OutboxEvent event = outboxEventRepository.findAll().get(0);
        assertThat(event.getCurrentStep()).isEqualTo(1); // checkpointed after step 0
        assertThat(event.getRetryCount()).isEqualTo(1);

        // Fix step 1 stub, retry
        stubCardProvider("PROV-RETRY-001");
        stubNotification("NOTIF-RETRY-001");

        poller.poll();

        // Only step 0 call (once) — credit bureau should not be called a second time
        wireMock.verify(1, com.github.tomakehurst.wiremock.client.WireMock
            .postRequestedFor(com.github.tomakehurst.wiremock.client.WireMock
                .urlEqualTo("/credit/check")));

        CardApplication finalApp = cardApplicationRepository.findById(
            outboxEventRepository.findAll().get(0).getCorrelationId()
        ).orElseThrow();
        assertThat(finalApp.getStatus()).isEqualTo(CardApplication.Status.COMPLETED);
    }

    @Test
    @DisplayName("Permanent failure: after maxRetries event marked FAILED")
    void afterMaxRetries_eventMarkedFailed() {
        stubCreditBureauFailure();

        cardApplicationService.submitApplication(
            "Frank Green", "frank@example.com", new BigDecimal("40000")
        );

        // maxRetries = 3, so poll 3 times
        poller.poll();
        poller.poll();
        poller.poll();

        OutboxEvent event = outboxEventRepository.findAll().get(0);
        assertThat(event.getStatus()).isEqualTo(OutboxEvent.Status.FAILED);
        assertThat(event.getRetryCount()).isEqualTo(3);
    }

    // ── Multiple concurrent applications ──────────────────────────────────────

    @Test
    @DisplayName("Multiple applications: each processes independently in same poll batch")
    void multipleApplications_processIndependently() {
        stubAllApisSuccess();

        cardApplicationService.submitApplication("User One", "u1@example.com", new BigDecimal("50000"));
        cardApplicationService.submitApplication("User Two", "u2@example.com", new BigDecimal("60000"));
        cardApplicationService.submitApplication("User Three", "u3@example.com", new BigDecimal("70000"));

        assertThat(outboxEventRepository.count()).isEqualTo(3);

        poller.poll();

        long completedCount = cardApplicationRepository.findAll().stream()
            .filter(a -> a.getStatus() == CardApplication.Status.COMPLETED)
            .count();
        assertThat(completedCount).isEqualTo(3);

        long processedOutbox = outboxEventRepository.findAll().stream()
            .filter(e -> e.getStatus() == OutboxEvent.Status.PROCESSED)
            .count();
        assertThat(processedOutbox).isEqualTo(3);
    }
}
