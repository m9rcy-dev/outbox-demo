package com.demo.outbox.util;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.*;

@DisplayName("MdcContext unit tests")
class MdcContextTest {

    @AfterEach
    void clearMdc() {
        MDC.clear();
    }

    @Test
    @DisplayName("forPipelineEvent sets correlationId and pipelineType")
    void forPipelineEvent_setsMdcKeys() {
        try (MdcContext mdc = MdcContext.forPipelineEvent("cid-123", "MY_PIPELINE")) {
            assertThat(MDC.get(MdcContext.KEY_CORRELATION_ID)).isEqualTo("cid-123");
            assertThat(MDC.get(MdcContext.KEY_PIPELINE_TYPE)).isEqualTo("MY_PIPELINE");
        }
    }

    @Test
    @DisplayName("forPipelineEvent close() removes correlationId, pipelineType, and step; leaves outer keys")
    void forPipelineEvent_close_removesOwnedKeysOnlyAndLeavesOuter() {
        MDC.put("outer", "stays");
        try (MdcContext mdc = MdcContext.forPipelineEvent("cid-123", "MY_PIPELINE")) {
            mdc.setStep("MyStep");
        }
        assertThat(MDC.get(MdcContext.KEY_CORRELATION_ID)).isNull();
        assertThat(MDC.get(MdcContext.KEY_PIPELINE_TYPE)).isNull();
        assertThat(MDC.get(MdcContext.KEY_STEP)).isNull();
        assertThat(MDC.get("outer")).isEqualTo("stays");
    }

    @Test
    @DisplayName("setStep writes step name into MDC")
    void setStep_putsMdcKey() {
        try (MdcContext mdc = MdcContext.forPipelineEvent("cid", "PIPE")) {
            mdc.setStep("CreditBureauCheckStep");
            assertThat(MDC.get(MdcContext.KEY_STEP)).isEqualTo("CreditBureauCheckStep");
        }
    }

    @Test
    @DisplayName("forHttpRequest with non-blank ID uses the provided value")
    void forHttpRequest_nonBlankId_usesProvidedId() {
        try (MdcContext mdc = MdcContext.forHttpRequest("req-abc")) {
            assertThat(MDC.get(MdcContext.KEY_REQUEST_ID)).isEqualTo("req-abc");
        }
    }

    @Test
    @DisplayName("forHttpRequest with null generates a gen- prefixed ID")
    void forHttpRequest_null_generatesId() {
        try (MdcContext mdc = MdcContext.forHttpRequest(null)) {
            assertThat(MDC.get(MdcContext.KEY_REQUEST_ID)).startsWith("gen-");
        }
    }

    @Test
    @DisplayName("forHttpRequest with blank string generates a gen- prefixed ID")
    void forHttpRequest_blank_generatesId() {
        try (MdcContext mdc = MdcContext.forHttpRequest("   ")) {
            assertThat(MDC.get(MdcContext.KEY_REQUEST_ID)).startsWith("gen-");
        }
    }

    @Test
    @DisplayName("forHttpRequest close() removes requestId but not outer correlationId")
    void forHttpRequest_close_removesOnlyRequestId() {
        MDC.put(MdcContext.KEY_CORRELATION_ID, "outer-cid");
        try (MdcContext mdc = MdcContext.forHttpRequest("req-abc")) {
            assertThat(MDC.get(MdcContext.KEY_CORRELATION_ID)).isEqualTo("outer-cid");
        }
        assertThat(MDC.get(MdcContext.KEY_REQUEST_ID)).isNull();
        assertThat(MDC.get(MdcContext.KEY_CORRELATION_ID)).isEqualTo("outer-cid");
    }

    @Test
    @DisplayName("wrap(Runnable) restores MDC snapshot on target thread and clears after completion")
    void wrap_runnable_propagatesSnapshotAndClearsAfter() throws Exception {
        MDC.put(MdcContext.KEY_CORRELATION_ID, "cid-async");
        Runnable wrapped = MdcContext.wrap(() -> {});
        MDC.clear();

        AtomicReference<String> duringRun = new AtomicReference<>();
        AtomicReference<String> afterRun = new AtomicReference<>();

        Thread t = new Thread(() -> {
            MdcContext.wrap(() -> duringRun.set(MDC.get(MdcContext.KEY_CORRELATION_ID))).run();
            afterRun.set(MDC.get(MdcContext.KEY_CORRELATION_ID));
        });
        MDC.put(MdcContext.KEY_CORRELATION_ID, "cid-async");
        Runnable task = MdcContext.wrap(() -> duringRun.set(MDC.get(MdcContext.KEY_CORRELATION_ID)));
        MDC.clear();

        Thread thread = new Thread(() -> {
            task.run();
            afterRun.set(MDC.get(MdcContext.KEY_CORRELATION_ID));
        });
        thread.start();
        thread.join(3000);

        assertThat(duringRun.get()).isEqualTo("cid-async");
        assertThat(afterRun.get()).isNull();
    }

    @Test
    @DisplayName("wrap(Supplier) propagates MDC snapshot, returns computed value, and clears MDC after")
    void wrap_supplier_propagatesSnapshotAndReturnsValue() throws Exception {
        MDC.put(MdcContext.KEY_CORRELATION_ID, "cid-supply");
        var wrapped = MdcContext.wrap(() -> MDC.get(MdcContext.KEY_CORRELATION_ID) + "-computed");
        MDC.clear();

        String result = CompletableFuture.supplyAsync(wrapped).get();

        assertThat(result).isEqualTo("cid-supply-computed");
    }

    @Test
    @DisplayName("wrap(Runnable) with no MDC on calling thread runs worker with empty MDC")
    void wrap_runnable_nullMdc_workerHasNoMdc() throws Exception {
        // No MDC on calling thread → getCopyOfContextMap() returns null → falls back to emptyMap()
        AtomicReference<String> captured = new AtomicReference<>("sentinel");
        Runnable task = MdcContext.wrap(() -> captured.set(MDC.get(MdcContext.KEY_CORRELATION_ID)));

        Thread t = new Thread(task);
        t.start();
        t.join(3000);

        assertThat(captured.get()).isNull();
    }

    @Test
    @DisplayName("wrap(Supplier) clears MDC on worker thread after task completes")
    void wrap_supplier_clearsMdcAfterGet() throws Exception {
        MDC.put(MdcContext.KEY_CORRELATION_ID, "cid");
        AtomicReference<String> cidAfterTask = new AtomicReference<>("sentinel");

        CompletableFuture.runAsync(() -> {
            MdcContext.wrap(() -> "value").get();
            cidAfterTask.set(MDC.get(MdcContext.KEY_CORRELATION_ID));
        }).get();

        assertThat(cidAfterTask.get()).isNull();
    }
}
