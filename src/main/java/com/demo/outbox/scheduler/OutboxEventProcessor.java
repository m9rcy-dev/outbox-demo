package com.demo.outbox.scheduler;

import com.demo.outbox.entity.OutboxEvent;
import com.demo.outbox.pipeline.PipelineRegistry;
import com.demo.outbox.pipeline.PipelineStep;
import com.demo.outbox.util.MdcContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Saga orchestrator for outbox-backed pipelines.
 *
 * Coordinates the step loop without owning a transaction itself. Each step is
 * delegated to {@link OutboxStepExecutor}, which commits the step's work and its
 * checkpoint independently. A failure at step N preserves the committed state of
 * steps 0..N-1 — the pipeline resumes from the last checkpoint on the next poll.
 *
 * OptimisticLockingFailureException from a concurrent pod is not caught here;
 * it propagates to {@link PipelineOutboxPoller} which logs and skips that event.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class OutboxEventProcessor {

    private final PipelineRegistry pipelineRegistry;
    private final OutboxStepExecutor stepExecutor;

    @SuppressWarnings({"unchecked", "rawtypes"})
    public void process(OutboxEvent staleRef, MdcContext mdc) {
        OutboxEvent event = stepExecutor.loadAndValidate(staleRef.getId());
        if (event == null) return;

        List<PipelineStep<?>> steps = pipelineRegistry.getSteps(event.getPipelineType());
        if (steps.isEmpty()) {
            stepExecutor.markFailed(event.getId(),
                "No steps found for pipeline: " + event.getPipelineType());
            return;
        }

        for (int i = event.getCurrentStep(); i < steps.size(); i++) {
            PipelineStep step = steps.get(i);
            mdc.setStep(step.getClass().getSimpleName());

            log.info("Pipeline [{}] correlationId={} → executing step {}/{} ({})",
                event.getPipelineType(),
                event.getCorrelationId(),
                i + 1,
                steps.size(),
                step.getClass().getSimpleName()
            );

            boolean continueExecution = stepExecutor.executeStep(
                event.getId(), step, i, steps.size());

            if (!continueExecution) {
                log.info("Pipeline [{}] halted at step {} for correlationId={}",
                    event.getPipelineType(), i + 1, event.getCorrelationId());
                return;
            }
        }

        log.info("Pipeline [{}] COMPLETED for correlationId={}",
            event.getPipelineType(), event.getCorrelationId());
    }
}
