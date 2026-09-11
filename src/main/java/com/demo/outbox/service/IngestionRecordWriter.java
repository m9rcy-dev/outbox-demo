package com.demo.outbox.service;

import com.demo.outbox.entity.IngestionRecord;
import com.demo.outbox.pipeline.context.DataIngestionContext;
import com.demo.outbox.pipeline.step.ingestion.DetokenisationStep;
import com.demo.outbox.repository.IngestionRecordRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Owns the atomic write for a new ingestion submission: business row +
 * outbox event, in one transaction.
 *
 * Kept as a separate bean from {@link DataIngestionService} — not a
 * convenience, a requirement: {@link DataIngestionService#submitRecord} needs
 * to catch the unique-constraint violation raised by a duplicate
 * {@code idempotencyKey} *after* this transaction has rolled back, and Spring's
 * {@code @Transactional} proxy only applies to calls that cross a bean
 * boundary. A self-invoked call (a method calling another {@code @Transactional}
 * method on {@code this}) silently skips the proxy — the method would run with
 * no transaction at all, and the exception would surface mid-transaction
 * instead of after a clean rollback. Same rationale as
 * {@link com.demo.outbox.scheduler.OutboxStepExecutor} being split out of
 * {@link com.demo.outbox.scheduler.OutboxEventProcessor}.
 */
@Service
@RequiredArgsConstructor
class IngestionRecordWriter {

    private final IngestionRecordRepository ingestionRecordRepository;
    private final OutboxService outboxService;

    @Transactional
    public IngestionRecord insert(String token, String cardholderPhone, String idempotencyKey) {
        IngestionRecord record = IngestionRecord.builder()
            .token(token)
            .cardholderPhone(cardholderPhone)
            .idempotencyKey(idempotencyKey)
            .status(IngestionRecord.Status.RECEIVED)
            .build();
        // saveAndFlush forces the unique-constraint check to run now, inside this
        // transaction, so DataIngestionService.submitRecord sees the
        // DataIntegrityViolationException immediately after a clean rollback —
        // not deferred to commit time, and never after the outbox event below
        // has also been written.
        record = ingestionRecordRepository.saveAndFlush(record);

        DataIngestionContext context = DataIngestionContext.builder()
            .recordId(record.getId())
            .token(token)
            .cardholderPhone(cardholderPhone)
            .build();

        outboxService.saveEvent(
            DetokenisationStep.PIPELINE_TYPE,
            record.getId(),
            context
        );

        return record;
    }
}
