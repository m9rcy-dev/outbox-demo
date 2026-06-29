package com.demo.outbox.service;

import com.demo.outbox.entity.OutboxEvent;
import com.demo.outbox.pipeline.PipelineRegistry;
import com.demo.outbox.repository.OutboxEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.UUID;

/**
 * Call {@link #saveEvent} INSIDE an existing {@code @Transactional} method.
 *
 * The outbox row is committed atomically with your business data — if the
 * business transaction rolls back, the outbox row rolls back too.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class OutboxService {

    private final OutboxEventRepository outboxEventRepository;
    private final PipelineRegistry pipelineRegistry;
    private final ObjectMapper objectMapper;

    /**
     * Persist a pipeline outbox event.
     *
     * @param pipelineType must match a registered pipeline type
     * @param correlationId business correlation ID (e.g. applicationId cast to UUID)
     * @param context       the initial pipeline context — serialised to JSON
     */
    public OutboxEvent saveEvent(String pipelineType, UUID correlationId, Object context) {
        if (!pipelineRegistry.hasPipeline(pipelineType)) {
            throw new IllegalArgumentException(
                "No pipeline registered for type: " + pipelineType);
        }

        int totalSteps = pipelineRegistry.getSteps(pipelineType).size();

        try {
            OutboxEvent event = OutboxEvent.builder()
                .correlationId(correlationId)
                .pipelineType(pipelineType)
                .currentStep(0)
                .totalSteps(totalSteps)
                .status(OutboxEvent.Status.PENDING)
                .payload(objectMapper.writeValueAsString(context))
                .build();

            OutboxEvent saved = outboxEventRepository.save(event);
            log.debug("Outbox event created id={} pipeline={} correlationId={}",
                saved.getId(), pipelineType, correlationId);
            return saved;

        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to serialize pipeline context", e);
        }
    }
}
