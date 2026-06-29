package com.demo.outbox.scheduler;

import com.demo.outbox.config.AppConfig;
import com.demo.outbox.entity.OutboxEvent;
import com.demo.outbox.repository.OutboxEventRepository;
import com.demo.outbox.util.MdcContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.util.List;

/**
 * Polls the outbox table and drives pipeline execution step by step.
 *
 * Holds a PESSIMISTIC_WRITE lock on the batch fetch to prevent double-execution
 * across pods. Per-event processing is delegated to {@link OutboxEventProcessor},
 * which coordinates the saga loop. Each step commits its own REQUIRES_NEW
 * transaction via {@link OutboxStepExecutor}, so failures at step N preserve the
 * committed checkpoints of prior steps.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class PipelineOutboxPoller {

    private final OutboxEventRepository outboxEventRepository;
    private final OutboxEventProcessor outboxEventProcessor;
    private final AppConfig.AppProperties properties;

    @Scheduled(fixedDelayString = "${app.outbox.poll-delay-ms:5000}")
    public void poll() {
        int batchSize = properties.getOutbox().getPollBatchSize();
        List<OutboxEvent> events = outboxEventRepository.findPendingForProcessing(batchSize);

        if (!events.isEmpty()) {
            log.debug("Outbox poll: found {} PENDING event(s)", events.size());
        }

        for (OutboxEvent event : events) {
            try (MdcContext mdc = MdcContext.forPipelineEvent(
                    event.getCorrelationId().toString(), event.getPipelineType())) {
                outboxEventProcessor.process(event, mdc);
            } catch (OptimisticLockingFailureException e) {
                // Another pod committed first; the REQUIRES_NEW transaction was rolled back cleanly.
                // Skip this event — the winning pod has it covered.
                log.debug("Event {} had concurrent update — skipping, another pod owns it", event.getId());
            }
        }
    }
}
