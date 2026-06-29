package com.demo.outbox.config;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.*;

@DisplayName("AsyncConfig unit tests")
class AsyncConfigTest {

    private final AsyncConfig config = new AsyncConfig();

    @AfterEach
    void clearMdc() {
        MDC.clear();
    }

    @Test
    @DisplayName("getAsyncExecutor returns a non-null configured executor")
    void getAsyncExecutor_returnsNonNull() {
        Executor executor = config.getAsyncExecutor();
        assertThat(executor).isNotNull();
    }

    @Test
    @DisplayName("TaskDecorator propagates MDC snapshot to worker thread")
    void taskDecorator_propagatesMdcToWorkerThread() throws Exception {
        Executor executor = config.getAsyncExecutor();
        MDC.put("correlationId", "async-cid");

        AtomicReference<String> captured = new AtomicReference<>();
        CompletableFuture.runAsync(
            () -> captured.set(MDC.get("correlationId")), executor
        ).get(5, TimeUnit.SECONDS);

        assertThat(captured.get()).isEqualTo("async-cid");
    }

    @Test
    @DisplayName("TaskDecorator with no MDC on calling thread runs worker with empty MDC")
    void taskDecorator_noMdcOnCallingThread_workerHasNoMdc() throws Exception {
        Executor executor = config.getAsyncExecutor();
        // No MDC set on calling thread — snapshot will be null, falling back to emptyMap()

        AtomicReference<String> captured = new AtomicReference<>("sentinel");
        CompletableFuture.runAsync(
            () -> captured.set(MDC.get("correlationId")), executor
        ).get(5, TimeUnit.SECONDS);

        assertThat(captured.get()).isNull();
    }
}
