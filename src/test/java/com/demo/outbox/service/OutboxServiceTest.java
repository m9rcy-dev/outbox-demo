package com.demo.outbox.service;

import com.demo.outbox.entity.OutboxEvent;
import com.demo.outbox.pipeline.PipelineRegistry;
import com.demo.outbox.pipeline.PipelineStep;
import com.demo.outbox.pipeline.context.CardApplicationContext;
import com.demo.outbox.pipeline.step.CreditBureauCheckStep;
import com.demo.outbox.repository.OutboxEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("OutboxService unit tests")
class OutboxServiceTest {

    @Mock OutboxEventRepository outboxEventRepository;
    @Mock PipelineRegistry pipelineRegistry;

    @InjectMocks OutboxService outboxService;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private UUID correlationId;
    private CardApplicationContext context;

    @BeforeEach
    void inject() throws Exception {
        // Inject real ObjectMapper via reflection (InjectMocks doesn't handle it)
        var field = OutboxService.class.getDeclaredField("objectMapper");
        field.setAccessible(true);
        field.set(outboxService, objectMapper);

        correlationId = UUID.randomUUID();
        context = CardApplicationContext.builder()
            .applicationId(correlationId)
            .applicantName("Test User")
            .email("test@example.com")
            .annualIncome(new BigDecimal("60000"))
            .build();
    }

    @Test
    @DisplayName("saveEvent persists outbox row with correct fields")
    void saveEvent_persistsOutboxRow() {
        when(pipelineRegistry.hasPipeline(CreditBureauCheckStep.PIPELINE_TYPE)).thenReturn(true);
        when(pipelineRegistry.getSteps(CreditBureauCheckStep.PIPELINE_TYPE))
            .thenReturn(List.of(mock(PipelineStep.class), mock(PipelineStep.class)));
        when(outboxEventRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        OutboxEvent saved = outboxService.saveEvent(
            CreditBureauCheckStep.PIPELINE_TYPE, correlationId, context
        );

        assertThat(saved.getPipelineType()).isEqualTo(CreditBureauCheckStep.PIPELINE_TYPE);
        assertThat(saved.getCorrelationId()).isEqualTo(correlationId);
        assertThat(saved.getCurrentStep()).isZero();
        assertThat(saved.getTotalSteps()).isEqualTo(2);
        assertThat(saved.getStatus()).isEqualTo(OutboxEvent.Status.PENDING);
        assertThat(saved.getPayload()).contains("test@example.com");
    }

    @Test
    @DisplayName("saveEvent throws for unknown pipeline type")
    void saveEvent_throwsForUnknownPipeline() {
        when(pipelineRegistry.hasPipeline("UNKNOWN_PIPELINE")).thenReturn(false);

        assertThatThrownBy(() ->
            outboxService.saveEvent("UNKNOWN_PIPELINE", correlationId, context)
        ).isInstanceOf(IllegalArgumentException.class)
         .hasMessageContaining("No pipeline registered");
    }

    @Test
    @DisplayName("saveEvent wraps JsonProcessingException in IllegalArgumentException")
    void saveEvent_wrapsSerializationException() throws Exception {
        ObjectMapper brokenMapper = mock(ObjectMapper.class);
        when(brokenMapper.writeValueAsString(any()))
            .thenThrow(new JsonProcessingException("broken") {});

        var field = OutboxService.class.getDeclaredField("objectMapper");
        field.setAccessible(true);
        field.set(outboxService, brokenMapper);

        when(pipelineRegistry.hasPipeline(CreditBureauCheckStep.PIPELINE_TYPE)).thenReturn(true);
        when(pipelineRegistry.getSteps(CreditBureauCheckStep.PIPELINE_TYPE))
            .thenReturn(List.of(mock(PipelineStep.class)));

        assertThatThrownBy(() ->
            outboxService.saveEvent(CreditBureauCheckStep.PIPELINE_TYPE, correlationId, context)
        ).isInstanceOf(IllegalArgumentException.class)
         .hasMessageContaining("Failed to serialize");
    }
}
