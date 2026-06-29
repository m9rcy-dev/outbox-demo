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
}
