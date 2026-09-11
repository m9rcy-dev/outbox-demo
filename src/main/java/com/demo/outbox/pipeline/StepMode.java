package com.demo.outbox.pipeline;

/**
 * Declares how a {@link PipelineStep}'s failures should be treated by
 * {@link com.demo.outbox.scheduler.OutboxStepExecutor}.
 *
 * The checkpoint/retry/dead-letter machinery is identical for every mode —
 * only what counts as "the step failed" and what happens next differs.
 */
public enum StepMode {

    /**
     * Default. The step must succeed before the pipeline continues.
     * Failures increment {@code OutboxEvent.retryCount} against the
     * pipeline-wide {@code app.outbox.max-retries} (unless the step
     * overrides {@link PipelineStep#getMaxRetries()}) and the event is
     * dead-lettered to FAILED once exhausted.
     */
    BLOCKING_REQUIRED,

    /**
     * The step must still succeed before the pipeline continues, but its
     * downstream client already owns retry/backoff (e.g. a resilient SDK,
     * or a queue with its own redelivery policy). By the time an exception
     * reaches the pipeline, the client has already exhausted its own
     * attempts — so the pipeline should not stack many more retries on top.
     * Use {@link PipelineStep#getMaxRetries()} to set a low, step-specific
     * ceiling instead of inheriting the global default.
     */
    BLOCKING_SELF_RETRIED,

    /**
     * Best-effort. A failure is logged and the checkpoint still advances —
     * the pipeline proceeds to the next step as if this one succeeded.
     * retryCount is not consumed, so this step's failures never dead-letter
     * the event. Use for steps whose outcome nothing downstream depends on
     * (e.g. an SMS confirmation, an internal audit note).
     */
    FIRE_AND_FORGET
}
