package com.demo.outbox.entity;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Business record for the DATA_INGESTION_PIPELINE — mirrors CardApplication's
 * role for CARD_APPLICATION_PIPELINE: written atomically with the outbox
 * event, then progressively updated by the pipeline steps that require it.
 *
 * Steps that are FIRE_AND_FORGET (internal notes, SMS) do not gate status
 * transitions here — a failure in those steps still lets the record reach
 * INGESTED.
 */
@Entity
@Table(name = "ingestion_record")
@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class IngestionRecord {

    public enum Status {
        RECEIVED,             // initial state — small synchronous processing done, 202 returned
        DETOKENISED,          // step 0 done
        MASTERCARD_MATCHED,   // step 1 done
        INGESTED,             // all required steps done (fire-and-forget steps may or may not have succeeded)
        FAILED                // pipeline permanently failed
    }

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(nullable = false)
    private String token;

    private String cardholderPhone;

    /**
     * Caller-supplied de-duplication key (e.g. the {@code Idempotency-Key} HTTP
     * header). A DB-level unique constraint is the hard guarantee against two
     * pods both accepting the same duplicate request; nullable because callers
     * are not required to supply one. See {@link com.demo.outbox.service.DataIngestionService}.
     */
    @Column(unique = true)
    private String idempotencyKey;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private Status status = Status.RECEIVED;

    // Populated progressively by pipeline steps
    private String panLast4;
    private String mastercardMatchId;

    @CreationTimestamp
    private LocalDateTime createdAt;

    @UpdateTimestamp
    private LocalDateTime updatedAt;
}
