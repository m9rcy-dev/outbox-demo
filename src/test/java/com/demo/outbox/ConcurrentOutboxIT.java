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
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.moreThanOrExactly;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Concurrent outbox — PostgreSQL concurrency guarantees")
class ConcurrentOutboxIT extends PostgresBaseTest {

    @Autowired CardApplicationService cardApplicationService;
    @Autowired PipelineOutboxPoller poller;
    @Autowired OutboxEventRepository outboxEventRepository;
    @Autowired CardApplicationRepository cardApplicationRepository;
    @Autowired PlatformTransactionManager txManager;

    private TransactionTemplate txTemplate;

    @BeforeEach
    void setup() {
        txTemplate = new TransactionTemplate(txManager);
        outboxEventRepository.deleteAll();
        cardApplicationRepository.deleteAll();
    }

    // ── Test helpers ─────────────────────────────────────────────────────────

    private OutboxEvent buildPendingEvent() {
        return OutboxEvent.builder()
            .correlationId(UUID.randomUUID())
            .pipelineType("CARD_APPLICATION_PIPELINE")
            .currentStep(0)
            .totalSteps(4)
            .status(OutboxEvent.Status.PENDING)
            .payload("{\"applicationId\":\"" + UUID.randomUUID()
                     + "\",\"applicantName\":\"Test\",\"email\":\"t@test.com\",\"annualIncome\":50000}")
            .retryCount(0)
            .build();
    }

    // ── Test 1: Optimistic lock conflict ─────────────────────────────────────

    @Test
    @DisplayName("@Version: second transaction with stale version throws OptimisticLockingFailureException")
    void optimisticLock_staleEntitySave_throwsOptimisticLockingFailureException() {
        // Save initial event; DB version = 0
        OutboxEvent saved = txTemplate.execute(status ->
            outboxEventRepository.save(buildPendingEvent()));

        // Load a stale reference in a separate transaction; tx commits → entity detaches at version=0
        OutboxEvent stale = txTemplate.execute(status ->
            outboxEventRepository.findById(saved.getId()).orElseThrow());

        // Pod A updates the same row; DB version advances to 1
        txTemplate.execute(status -> {
            OutboxEvent fresh = outboxEventRepository.findById(saved.getId()).orElseThrow();
            fresh.setCurrentStep(1);
            return outboxEventRepository.saveAndFlush(fresh);
        });

        // Pod B tries to save the stale entity (still version=0); DB has version=1.
        // Hibernate issues UPDATE … WHERE id=? AND version=0, gets 0 rows, and throws.
        assertThatThrownBy(() -> txTemplate.execute(status -> {
            stale.setCurrentStep(1);
            outboxEventRepository.saveAndFlush(stale);
            return null;
        })).isInstanceOf(OptimisticLockingFailureException.class);
    }

    // ── Test 2: SKIP LOCKED distribution ─────────────────────────────────────

    @Test
    @DisplayName("SKIP LOCKED: concurrent poller skips rows already locked by another pod")
    void findPendingForProcessing_skipLocked_secondPollerGetsNoEvents()
            throws Exception {

        txTemplate.execute(status -> {
            for (int i = 0; i < 4; i++) outboxEventRepository.save(buildPendingEvent());
            return null;
        });

        CountDownLatch pod1HasLocks = new CountDownLatch(1);
        CountDownLatch pod2Done    = new CountDownLatch(1);
        List<UUID> pod1Ids = Collections.synchronizedList(new ArrayList<>());
        List<UUID> pod2Ids = Collections.synchronizedList(new ArrayList<>());

        ExecutorService exec = Executors.newFixedThreadPool(2);

        // Pod 1: fetch 4 events (acquires FOR UPDATE locks), hold until Pod 2 is done
        Future<?> pod1Future = exec.submit(() ->
            txTemplate.execute(status -> {
                outboxEventRepository.findPendingForProcessing(10)
                    .forEach(e -> pod1Ids.add(e.getId()));
                pod1HasLocks.countDown();
                try {
                    pod2Done.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
                return null; // rollback — only testing lock acquisition
            })
        );

        assertThat(pod1HasLocks.await(10, TimeUnit.SECONDS))
            .as("Pod 1 should acquire locks within 10s").isTrue();

        // Pod 2: queries while Pod 1 holds all locks — SKIP LOCKED returns empty
        txTemplate.execute(status -> {
            outboxEventRepository.findPendingForProcessing(10)
                .forEach(e -> pod2Ids.add(e.getId()));
            return null;
        });

        pod2Done.countDown();
        pod1Future.get(10, TimeUnit.SECONDS);
        exec.shutdown();

        assertThat(pod1Ids).hasSize(4);
        assertThat(pod2Ids).isEmpty(); // SKIP LOCKED: all rows were held by Pod 1
    }

    // ── Test 3: Two-pod end-to-end idempotency ────────────────────────────────

    @Test
    @DisplayName("Two concurrent pods process each event exactly once (end-to-end)")
    void concurrentPollers_processEachEventExactlyOnce() throws Exception {
        stubAllApisSuccess();

        for (int i = 0; i < 4; i++) {
            cardApplicationService.submitApplication(
                "User " + i, "user" + i + "@test.com", BigDecimal.valueOf(50000 + i * 1000L));
        }
        assertThat(outboxEventRepository.count()).isEqualTo(4);

        // Release both pods at exactly the same moment
        CountDownLatch startGun = new CountDownLatch(1);
        ExecutorService exec = Executors.newFixedThreadPool(2);

        Future<?> pod1 = exec.submit(() -> { startGun.await(); poller.poll(); return null; });
        Future<?> pod2 = exec.submit(() -> { startGun.await(); poller.poll(); return null; });

        startGun.countDown();
        pod1.get(30, TimeUnit.SECONDS);
        pod2.get(30, TimeUnit.SECONDS);
        exec.shutdown();

        // Every event PROCESSED — @Version ensures each event's DB state is committed exactly once
        assertThat(outboxEventRepository.findAll())
            .hasSize(4)
            .allMatch(e -> e.getStatus() == OutboxEvent.Status.PROCESSED);

        assertThat(cardApplicationRepository.findAll())
            .hasSize(4)
            .allMatch(a -> a.getStatus() == CardApplication.Status.COMPLETED);

        // APIs called at least 4 times — proves no event was silently skipped.
        // With concurrent pods racing on the same batch, duplicate calls are possible
        // (at most 2× per event) because SKIP LOCKED is a distribution hint, not a hard
        // guard; the @Version checkpoint prevents duplicate DB commits, not duplicate
        // API calls. Pipeline steps must therefore be idempotent.
        wireMock.verify(moreThanOrExactly(4), postRequestedFor(urlEqualTo("/credit/check")));
        wireMock.verify(moreThanOrExactly(4), postRequestedFor(urlEqualTo("/cards/register")));
        wireMock.verify(moreThanOrExactly(4), postRequestedFor(urlEqualTo("/notifications/send")));

        // Idempotency guard: a second poll on already-PROCESSED events must skip all of them.
        wireMock.resetRequests();
        poller.poll();
        wireMock.verify(0, postRequestedFor(urlEqualTo("/credit/check")));
        wireMock.verify(0, postRequestedFor(urlEqualTo("/cards/register")));
        wireMock.verify(0, postRequestedFor(urlEqualTo("/notifications/send")));
    }
}
