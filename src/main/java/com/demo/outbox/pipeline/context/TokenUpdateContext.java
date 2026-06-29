package com.demo.outbox.pipeline.context;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

/**
 * Shared mutable context for the TOKEN_UPDATE_PIPELINE.
 *
 *   Step 0 (MastercardTokenUpdateStep) → mastercardRef, updatedStatus
 *   Step 1 (LocalCardStatusSyncStep)   → reads updatedStatus, writes local DB
 *   Step 2 (MqPublishStep)             → messageId
 *   Step 3 (SmsNotificationStep)       → smsMessageId
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TokenUpdateContext {

    // ── Input (set when the outbox event is created) ─────────────────────────
    private UUID cardId;
    private String tokenId;
    private String desiredStatus;   // e.g. "SUSPENDED", "ACTIVATED"
    private String cardholderPhone;

    // ── Step 0 output ────────────────────────────────────────────────────────
    private String mastercardRef;   // Mastercard transaction reference
    private String updatedStatus;   // confirmed status from Mastercard response

    // ── Step 2 output ────────────────────────────────────────────────────────
    private String messageId;       // MQ message ID

    // ── Step 3 output ────────────────────────────────────────────────────────
    private String smsMessageId;
}
