package com.demo.outbox.pipeline.context;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

/**
 * Shared mutable context for the DATA_INGESTION_PIPELINE.
 *
 *   Step 0 (DetokenisationStep)  → panLast4          [BLOCKING_REQUIRED]
 *   Step 1 (MastercardSearchStep) → mastercardMatchId [BLOCKING_SELF_RETRIED]
 *   Step 2 (InternalNotesStep)   → notesId           [FIRE_AND_FORGET]
 *   Step 3 (SmsNotificationStep) → smsMessageId       [FIRE_AND_FORGET]
 *   Step 4 (MqPublishStep)       → messageId          [BLOCKING_SELF_RETRIED]
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DataIngestionContext {

    // ── Input (set when the outbox event is first created) ──────────────────
    private UUID recordId;
    private String token;             // opaque token to be detokenised in step 0
    private String cardholderPhone;

    // ── Step 0 output ────────────────────────────────────────────────────────
    private String panLast4;

    // ── Step 1 output ────────────────────────────────────────────────────────
    private String mastercardMatchId;

    // ── Step 2 output (best-effort) ─────────────────────────────────────────
    private String notesId;

    // ── Step 3 output (best-effort) ─────────────────────────────────────────
    private String smsMessageId;

    // ── Step 4 output ────────────────────────────────────────────────────────
    private String messageId;
}
