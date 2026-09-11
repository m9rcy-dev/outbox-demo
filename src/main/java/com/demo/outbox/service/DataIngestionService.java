package com.demo.outbox.service;

import com.demo.outbox.entity.IngestionRecord;
import com.demo.outbox.repository.IngestionRecordRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * Entry point for the DATA_INGESTION_PIPELINE.
 *
 * {@code submitRecord} does the small amount of synchronous processing the
 * caller needs before returning (persist the record + outbox event, both in
 * one transaction) and returns immediately. The 5-step pipeline —
 * detokenisation, Mastercard search, internal notes, SMS, MQ publish — runs
 * asynchronously via the poller, matching the 202-then-async-pipeline shape
 * used by {@link CardApplicationService}.
 *
 * <h2>Request-level idempotency</h2>
 * Retrying a step within one pipeline run is already checkpoint-safe (see
 * {@link com.demo.outbox.scheduler.OutboxStepExecutor}) — that protects
 * against re-running steps of the SAME outbox event. It does nothing for a
 * duplicate SUBMISSION: a client retry, or an upstream at-least-once
 * redelivery, would otherwise create a second {@link IngestionRecord} and a
 * second outbox event, running the whole 5-step pipeline twice.
 * {@code idempotencyKey} (e.g. an {@code Idempotency-Key} HTTP header)
 * closes that gap: the same key always returns the same record instead of
 * creating a new one.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class DataIngestionService {

    private final IngestionRecordRepository ingestionRecordRepository;
    private final IngestionRecordWriter ingestionRecordWriter;

    /**
     * @param idempotencyKey caller-supplied de-duplication key, or {@code null}
     *                       if the caller doesn't want de-duplication
     */
    public IngestionRecord submitRecord(String token, String cardholderPhone, String idempotencyKey) {
        if (idempotencyKey != null) {
            // Fast path: this is the common case for a genuine retry — the first
            // submission already committed, so no new row/event is created at all.
            var existing = ingestionRecordRepository.findByIdempotencyKey(idempotencyKey);
            if (existing.isPresent()) {
                log.info("Duplicate submission for idempotencyKey={} — returning existing record {}",
                    idempotencyKey, existing.get().getId());
                return existing.get();
            }
        }

        try {
            IngestionRecord record = ingestionRecordWriter.insert(token, cardholderPhone, idempotencyKey);
            log.info("Ingestion record {} submitted, pipeline queued", record.getId());
            return record;

        } catch (DataIntegrityViolationException e) {
            // Two pods raced on the same idempotencyKey between the check above and
            // the insert — the unique constraint on idempotency_key is the hard
            // guarantee, the findBy... check above is only a fast-path optimisation.
            // The loser's transaction rolled back cleanly; return the winner's row.
            if (idempotencyKey == null) {
                throw e; // not an idempotency collision — a real constraint failure
            }
            log.debug("Concurrent duplicate submission for idempotencyKey={} — returning the winner's record",
                idempotencyKey);
            return ingestionRecordRepository.findByIdempotencyKey(idempotencyKey)
                .orElseThrow(() -> e);
        }
    }

    @Transactional(readOnly = true)
    public IngestionRecord getRecord(UUID id) {
        return ingestionRecordRepository.findById(id)
            .orElseThrow(() -> new IllegalArgumentException("Ingestion record not found: " + id));
    }
}
