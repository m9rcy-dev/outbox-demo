package com.demo.outbox.entity;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;
import java.util.UUID;

// @Version enables optimistic locking: concurrent saves from different pods fail fast
// with OptimisticLockException rather than silently overwriting each other's state.

@Entity
@Table(name = "outbox_event")
@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class OutboxEvent {

    public enum Status { PENDING, PROCESSED, FAILED }

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Version
    private Long version;

    /** Ties all events for one logical business operation together */
    @Column(nullable = false)
    private UUID correlationId;

    /** Which pipeline to run — maps to PipelineStep.getPipelineType() */
    @Column(nullable = false)
    private String pipelineType;

    /** Index of the next step to execute (0-based) */
    @Column(nullable = false)
    private int currentStep = 0;

    @Column(nullable = false)
    private int totalSteps;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private Status status = Status.PENDING;

    /**
     * JSON-serialized pipeline context.
     * Mutated and persisted after each successful step — acts as a checkpoint.
     */
    @Column(nullable = false, columnDefinition = "TEXT")
    private String payload;

    private int retryCount = 0;
    private String lastError;

    @CreationTimestamp
    private LocalDateTime createdAt;
    private LocalDateTime processedAt;
}
