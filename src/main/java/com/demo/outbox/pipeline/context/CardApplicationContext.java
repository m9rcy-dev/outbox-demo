package com.demo.outbox.pipeline.context;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Shared mutable context for the CARD_APPLICATION_PIPELINE.
 *
 * Fields are populated progressively as each step completes:
 *   Step 0 (CreditBureauCheckStep)      → creditScore
 *   Step 1 (CardProviderRegisterStep)   → providerRef
 *   Step 2 (NotificationStep)           → notificationId
 *   Step 3 (FinalDbUpdateStep)          → reads all above, updates CardApplication
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CardApplicationContext {

    // ── Input (set when the outbox event is first created) ──────────────────
    private UUID applicationId;
    private String applicantName;
    private String email;
    private BigDecimal annualIncome;

    // ── Step 0 output ────────────────────────────────────────────────────────
    private Integer creditScore;

    // ── Step 1 output ────────────────────────────────────────────────────────
    private String providerRef;

    // ── Step 2 output ────────────────────────────────────────────────────────
    private String notificationId;
}
