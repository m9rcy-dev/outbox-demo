package com.demo.outbox.util;

import org.slf4j.MDC;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * AutoCloseable MDC context holder for try-with-resources blocks.
 *
 * Removes only the keys it owns on close() — outer MDC context is untouched.
 * Thread-propagation helpers (wrap) capture a snapshot of the current MDC at
 * call time and restore it on the target thread.
 *
 * Pipeline event usage:
 * <pre>
 *   try (MdcContext mdc = MdcContext.forPipelineEvent(correlationId, pipelineType)) {
 *       mdc.setStep("CreditBureauCheckStep");
 *       // correlationId, pipelineType, and step appear on every log line
 *   } // all three keys cleared
 * </pre>
 *
 * HTTP request usage:
 * <pre>
 *   try (MdcContext mdc = MdcContext.forHttpRequest(request.getHeader("X-Request-ID"))) {
 *       // requestId appears on every log line within this block
 *   }
 * </pre>
 *
 * Async propagation usage:
 * <pre>
 *   CompletableFuture.runAsync(MdcContext.wrap(() -> { ... }));
 *   CompletableFuture.supplyAsync(MdcContext.wrap(() -> computeValue()));
 * </pre>
 */
public final class MdcContext implements AutoCloseable {

    public static final String KEY_CORRELATION_ID = "correlationId";
    public static final String KEY_PIPELINE_TYPE  = "pipelineType";
    public static final String KEY_STEP           = "step";
    public static final String KEY_REQUEST_ID     = "requestId";

    private final boolean ownsCorrelationId;
    private final boolean ownsPipelineType;
    private final boolean ownsStep;
    private final boolean ownsRequestId;

    private MdcContext(String correlationId, String pipelineType,
                       String requestId, boolean managesStep) {
        this.ownsCorrelationId = correlationId != null;
        this.ownsPipelineType  = pipelineType  != null;
        this.ownsStep          = managesStep;
        this.ownsRequestId     = requestId     != null;

        if (ownsCorrelationId) MDC.put(KEY_CORRELATION_ID, correlationId);
        if (ownsPipelineType)  MDC.put(KEY_PIPELINE_TYPE,  pipelineType);
        if (ownsRequestId)     MDC.put(KEY_REQUEST_ID,     requestId);
    }

    /**
     * For the pipeline poller. Sets correlationId and pipelineType immediately.
     * Also takes ownership of the step key so close() removes it even though
     * step is written later via setStep().
     */
    public static MdcContext forPipelineEvent(String correlationId, String pipelineType) {
        return new MdcContext(correlationId, pipelineType, null, true);
    }

    /**
     * For the REST controller. Sets requestId.
     * If rawRequestId is null or blank a UUID is generated with a "gen-" prefix.
     */
    public static MdcContext forHttpRequest(String rawRequestId) {
        String id = (rawRequestId != null && !rawRequestId.isBlank())
                ? rawRequestId
                : "gen-" + UUID.randomUUID();
        return new MdcContext(null, null, id, false);
    }

    /**
     * Updates the step key in MDC as the pipeline advances through steps.
     * Safe to call multiple times. The key is always removed on close() for
     * pipeline event contexts.
     */
    public void setStep(String stepName) {
        MDC.put(KEY_STEP, stepName);
    }

    /**
     * Wraps a Runnable to propagate the current MDC snapshot to another thread.
     *
     * <pre>
     *   CompletableFuture.runAsync(MdcContext.wrap(() -> { ... }));
     * </pre>
     *
     * The wrapped Runnable clears MDC entirely on completion so it does not
     * leak into subsequent tasks on the same pooled thread.
     */
    public static Runnable wrap(Runnable runnable) {
        Map<String, String> snapshot = MDC.getCopyOfContextMap();
        return () -> {
            MDC.setContextMap(snapshot != null ? snapshot : Collections.emptyMap());
            try {
                runnable.run();
            } finally {
                MDC.clear();
            }
        };
    }

    /**
     * Wraps a Supplier to propagate the current MDC snapshot to another thread.
     *
     * <pre>
     *   CompletableFuture.supplyAsync(MdcContext.wrap(() -> computeValue()));
     * </pre>
     */
    public static <T> Supplier<T> wrap(Supplier<T> supplier) {
        Map<String, String> snapshot = MDC.getCopyOfContextMap();
        return () -> {
            MDC.setContextMap(snapshot != null ? snapshot : Collections.emptyMap());
            try {
                return supplier.get();
            } finally {
                MDC.clear();
            }
        };
    }

    @Override
    public void close() {
        if (ownsCorrelationId) MDC.remove(KEY_CORRELATION_ID);
        if (ownsPipelineType)  MDC.remove(KEY_PIPELINE_TYPE);
        if (ownsStep)          MDC.remove(KEY_STEP);
        if (ownsRequestId)     MDC.remove(KEY_REQUEST_ID);
    }
}
