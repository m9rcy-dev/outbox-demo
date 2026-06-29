package com.demo.outbox.service;

import com.demo.outbox.entity.CardApplication;
import com.demo.outbox.pipeline.context.CardApplicationContext;
import com.demo.outbox.pipeline.step.CreditBureauCheckStep;
import com.demo.outbox.repository.CardApplicationRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Demonstrates the Outbox Pattern in action.
 *
 * {@code submitApplication} saves the business record AND the outbox event
 * in one atomic transaction. No external API call happens here — that is
 * deferred to the pipeline poller.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class CardApplicationService {

    private final CardApplicationRepository cardApplicationRepository;
    private final OutboxService outboxService;

    /**
     * Atomically:
     *  1. Persists a new CardApplication (status=SUBMITTED)
     *  2. Creates an OutboxEvent that drives the 4-step pipeline
     *
     * Returns immediately — pipeline runs asynchronously via the poller.
     */
    @Transactional
    public CardApplication submitApplication(String applicantName,
                                             String email,
                                             BigDecimal annualIncome) {
        // Step A: save business entity
        CardApplication application = CardApplication.builder()
            .applicantName(applicantName)
            .email(email)
            .annualIncome(annualIncome)
            .status(CardApplication.Status.SUBMITTED)
            .build();
        application = cardApplicationRepository.save(application);

        // Step B: save outbox event — atomic with the above save
        CardApplicationContext context = CardApplicationContext.builder()
            .applicationId(application.getId())
            .applicantName(applicantName)
            .email(email)
            .annualIncome(annualIncome)
            .build();

        outboxService.saveEvent(
            CreditBureauCheckStep.PIPELINE_TYPE,
            application.getId(),
            context
        );

        log.info("Application {} submitted for {}, pipeline queued", application.getId(), email);
        return application;
    }

    /**
     * Simple status check — no join to outbox needed because status is
     * mirrored progressively onto the entity by each pipeline step.
     */
    @Transactional(readOnly = true)
    public CardApplication getApplication(UUID id) {
        return cardApplicationRepository.findById(id)
            .orElseThrow(() -> new IllegalArgumentException("Application not found: " + id));
    }
}
