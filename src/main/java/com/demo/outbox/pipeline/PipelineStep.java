package com.demo.outbox.pipeline;

/**
 * Generic contract for a single step in an outbox pipeline.
 *
 * @param <C> the shared context type passed between steps
 */
public interface PipelineStep<C> {

    /** Pipeline this step belongs to — must match the outbox_event.pipeline_type value */
    String getPipelineType();

    /** 0-based position in the pipeline */
    int getStepIndex();

    /** The context class so the executor can deserialize the JSON payload */
    Class<C> getContextClass();

    /**
     * Execute this step.
     *
     * @param context shared mutable context — read inputs set by previous steps,
     *                write outputs for subsequent steps
     * @return {@code true} to continue to the next step,
     *         {@code false} to stop the pipeline without error (e.g. early exit)
     * @throws Exception any exception marks the step as failed and triggers retry
     */
    boolean execute(C context) throws Exception;

    /**
     * How {@link com.demo.outbox.scheduler.OutboxStepExecutor} should treat a
     * failure of this step. Defaults to {@link StepMode#BLOCKING_REQUIRED} —
     * override for steps whose client owns its own retry, or whose outcome is
     * best-effort. See {@link StepMode} for the full contract of each value.
     */
    default StepMode getMode() {
        return StepMode.BLOCKING_REQUIRED;
    }

    /**
     * Per-step override for how many pipeline-level retries this step gets
     * before the event is dead-lettered (ignored for {@link StepMode#FIRE_AND_FORGET}).
     * Return a positive number to override; the default ({@code -1}) falls
     * back to {@code app.outbox.max-retries}.
     */
    default int getMaxRetries() {
        return -1;
    }

    /**
     * Whether this step is relevant for the given context. Checked before
     * {@link #execute} on every attempt — if it returns {@code false}, the
     * step is skipped entirely (not executed, not retried, not counted as a
     * failure) and the checkpoint advances as if it had succeeded.
     *
     * Default {@code true} — every existing step runs unconditionally unless
     * it opts in to filtering. Use this for steps that only make sense for
     * some shapes of data within one pipeline (e.g. an SMS step that only
     * applies when a phone number was actually supplied), not as a substitute
     * for {@link StepMode} — that governs how a step's *failure* is treated,
     * this governs whether it runs at all.
     */
    default boolean appliesTo(C context) {
        return true;
    }
}
