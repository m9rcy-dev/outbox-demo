package com.demo.outbox.scheduler;

import com.demo.outbox.config.AppConfig;
import com.demo.outbox.entity.OutboxEvent;
import com.demo.outbox.pipeline.PipelineStep;
import com.demo.outbox.repository.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Owns the transaction boundary for a single pipeline step.
 *
 * Each public method runs in REQUIRES_NEW so its commit is independent of any
 * surrounding call. This is the core of the saga guarantee: a failure at step N
 * rolls back only step N's work — steps 0..N-1 remain committed.
 *
 * Kept separate from OutboxEventProcessor because Spring's @Transactional proxy
 * only intercepts calls that cross bean boundaries.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class OutboxStepExecutor {

    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;
    private final AppConfig.AppProperties properties;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public OutboxEvent loadAndValidate(UUID eventId) {
        OutboxEvent event = outboxEventRepository.findById(eventId).orElse(null);
        if (event == null || event.getStatus() != OutboxEvent.Status.PENDING) {
            log.debug("Skipping event {} — status={}",
                eventId, event == null ? "deleted" : event.getStatus());
            return null;
        }
        return event;
    }

    /**
     * Executes one step and commits its checkpoint atomically.
     *
     * The step's own DB writes (e.g. updating the business entity status) participate
     * in this transaction, so both the business state and the saga checkpoint are
     * committed together — or both roll back on failure.
     *
     * @return true to continue to the next step, false to halt the pipeline
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean executeStep(UUID eventId, PipelineStep step, int stepIndex, int totalSteps) {
        OutboxEvent event = outboxEventRepository.findById(eventId)
            .orElseThrow(() -> new IllegalStateException("Event not found: " + eventId));

        // Idempotency guard: another pod already committed this step between our
        // loadAndValidate call and this executeStep call.
        if (event.getCurrentStep() > stepIndex) {
            log.debug("Step {} already committed for event {} — skipping", stepIndex, eventId);
            return true;
        }

        try {
            Object context = objectMapper.readValue(event.getPayload(), step.getContextClass());
            boolean continueExecution = step.execute(context);

            boolean isLast = (stepIndex == totalSteps - 1) || !continueExecution;
            event.setCurrentStep(stepIndex + 1);
            event.setPayload(objectMapper.writeValueAsString(context));
            if (isLast) {
                event.setStatus(OutboxEvent.Status.PROCESSED);
                event.setProcessedAt(LocalDateTime.now());
            }
            // saveAndFlush forces the version-check UPDATE immediately so
            // OptimisticLockingFailureException is thrown here (inside the try/catch)
            // rather than at commit time (outside it).
            outboxEventRepository.saveAndFlush(event);
            return continueExecution;

        } catch (OptimisticLockingFailureException e) {
            // Re-throw so the REQUIRES_NEW TX rolls back cleanly.
            // poll() catches this outside the transaction boundary.
            log.debug("Event {} concurrent update at step {} — another pod owns it", eventId, stepIndex);
            throw e;
        } catch (Exception e) {
            return handleStepFailure(event, stepIndex, e);
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markFailed(UUID eventId, String reason) {
        OutboxEvent event = outboxEventRepository.findById(eventId).orElse(null);
        if (event == null || event.getStatus() != OutboxEvent.Status.PENDING) return;
        event.setStatus(OutboxEvent.Status.FAILED);
        event.setLastError(reason);
        outboxEventRepository.save(event);
        log.error("Event {} permanently failed: {}", eventId, reason);
    }

    private boolean handleStepFailure(OutboxEvent event, int stepIndex, Exception e) {
        int newRetryCount = event.getRetryCount() + 1;
        int maxRetries = properties.getOutbox().getMaxRetries();

        log.warn("Pipeline [{}] step {} failed (attempt {}/{}): {}",
            event.getPipelineType(), stepIndex, newRetryCount, maxRetries, e.getMessage());

        event.setRetryCount(newRetryCount);
        event.setLastError("Step " + stepIndex + ": " + e.getMessage());

        if (newRetryCount >= maxRetries) {
            event.setStatus(OutboxEvent.Status.FAILED);
            log.error("Pipeline [{}] PERMANENTLY FAILED at step {} for correlationId={}: {}",
                event.getPipelineType(), stepIndex, event.getCorrelationId(), e.getMessage());
        }
        outboxEventRepository.save(event);
        return false;
    }
}
