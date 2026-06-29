package com.demo.outbox.scheduler;

import com.demo.outbox.config.AppConfig;
import com.demo.outbox.entity.OutboxEvent;
import com.demo.outbox.pipeline.PipelineStep;
import com.demo.outbox.pipeline.context.CardApplicationContext;
import com.demo.outbox.repository.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.dao.OptimisticLockingFailureException;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@DisplayName("OutboxStepExecutor — per-step transaction behaviour")
class OutboxStepExecutorTest {

    @Mock OutboxEventRepository outboxEventRepository;
    @Mock AppConfig.AppProperties properties;
    @Mock AppConfig.AppProperties.Outbox outboxProps;
    @InjectMocks OutboxStepExecutor executor;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setup() throws Exception {
        var field = OutboxStepExecutor.class.getDeclaredField("objectMapper");
        field.setAccessible(true);
        field.set(executor, objectMapper);

        when(properties.getOutbox()).thenReturn(outboxProps);
        when(outboxProps.getMaxRetries()).thenReturn(3);
    }

    // ── loadAndValidate ────────────────────────────────────────────────────────

    @Test
    @DisplayName("loadAndValidate returns null when event does not exist")
    void loadAndValidate_eventMissing_returnsNull() {
        UUID id = UUID.randomUUID();
        when(outboxEventRepository.findById(id)).thenReturn(Optional.empty());

        assertThat(executor.loadAndValidate(id)).isNull();
    }

    @Test
    @DisplayName("loadAndValidate returns null when event is not PENDING")
    void loadAndValidate_eventNotPending_returnsNull() {
        OutboxEvent event = pendingEvent(0);
        event.setStatus(OutboxEvent.Status.PROCESSED);
        when(outboxEventRepository.findById(event.getId())).thenReturn(Optional.of(event));

        assertThat(executor.loadAndValidate(event.getId())).isNull();
    }

    @Test
    @DisplayName("loadAndValidate returns the event when it is PENDING")
    void loadAndValidate_eventPending_returnsEvent() {
        OutboxEvent event = pendingEvent(0);
        when(outboxEventRepository.findById(event.getId())).thenReturn(Optional.of(event));

        assertThat(executor.loadAndValidate(event.getId())).isSameAs(event);
    }

    // ── executeStep ────────────────────────────────────────────────────────────

    @Test
    @DisplayName("executeStep advances currentStep and saves checkpoint")
    void executeStep_stepSucceeds_checkpointsProgress() throws Exception {
        OutboxEvent event = pendingEvent(0);
        when(outboxEventRepository.findById(event.getId())).thenReturn(Optional.of(event));
        when(outboxEventRepository.saveAndFlush(any())).thenAnswer(inv -> inv.getArgument(0));

        PipelineStep<CardApplicationContext> step = mockStep(true);
        executor.executeStep(event.getId(), step, 0, 2);

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).saveAndFlush(captor.capture());
        assertThat(captor.getValue().getCurrentStep()).isEqualTo(1);
        assertThat(captor.getValue().getStatus()).isEqualTo(OutboxEvent.Status.PENDING);
    }

    @Test
    @DisplayName("executeStep marks PROCESSED on the last step")
    void executeStep_lastStep_marksProcessed() throws Exception {
        OutboxEvent event = pendingEvent(0);
        when(outboxEventRepository.findById(event.getId())).thenReturn(Optional.of(event));
        when(outboxEventRepository.saveAndFlush(any())).thenAnswer(inv -> inv.getArgument(0));

        PipelineStep<CardApplicationContext> step = mockStep(true);
        executor.executeStep(event.getId(), step, 0, 1); // totalSteps=1, so this is the last

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).saveAndFlush(captor.capture());
        assertThat(captor.getValue().getStatus()).isEqualTo(OutboxEvent.Status.PROCESSED);
        assertThat(captor.getValue().getProcessedAt()).isNotNull();
    }

    @Test
    @DisplayName("executeStep marks PROCESSED and returns false when step signals early exit")
    void executeStep_stepReturnsFalse_marksProcessedAndReturnsFalse() throws Exception {
        OutboxEvent event = pendingEvent(0);
        when(outboxEventRepository.findById(event.getId())).thenReturn(Optional.of(event));
        when(outboxEventRepository.saveAndFlush(any())).thenAnswer(inv -> inv.getArgument(0));

        PipelineStep<CardApplicationContext> step = mockStep(false);
        boolean result = executor.executeStep(event.getId(), step, 0, 2);

        assertThat(result).isFalse();
        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).saveAndFlush(captor.capture());
        assertThat(captor.getValue().getStatus()).isEqualTo(OutboxEvent.Status.PROCESSED);
    }

    @Test
    @DisplayName("executeStep skips execution and returns true when step already committed (idempotency)")
    void executeStep_alreadyCommitted_skipsAndReturnsTrue() throws Exception {
        OutboxEvent event = pendingEvent(1); // currentStep=1, we're asked to re-run stepIndex=0
        when(outboxEventRepository.findById(event.getId())).thenReturn(Optional.of(event));

        PipelineStep<CardApplicationContext> step = mockStep(true);
        boolean result = executor.executeStep(event.getId(), step, 0, 2);

        assertThat(result).isTrue();
        verify(step, never()).execute(any());
        verify(outboxEventRepository, never()).saveAndFlush(any());
    }

    @Test
    @DisplayName("executeStep increments retryCount and stays PENDING before maxRetries")
    void executeStep_stepFails_incrementsRetryCountStaysPending() throws Exception {
        OutboxEvent event = pendingEvent(0);
        when(outboxEventRepository.findById(event.getId())).thenReturn(Optional.of(event));
        when(outboxEventRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        PipelineStep<CardApplicationContext> step = mockStep(true);
        when(step.execute(any())).thenThrow(new RuntimeException("API timeout"));

        boolean result = executor.executeStep(event.getId(), step, 0, 2);

        assertThat(result).isFalse();
        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        OutboxEvent saved = captor.getValue();
        assertThat(saved.getStatus()).isEqualTo(OutboxEvent.Status.PENDING);
        assertThat(saved.getRetryCount()).isEqualTo(1);
        assertThat(saved.getLastError()).contains("API timeout");
    }

    @Test
    @DisplayName("executeStep marks FAILED when retryCount reaches maxRetries")
    void executeStep_stepFails_permanentlyFailsAtMaxRetries() throws Exception {
        OutboxEvent event = pendingEvent(0);
        event.setRetryCount(2); // one more failure = maxRetries(3)
        when(outboxEventRepository.findById(event.getId())).thenReturn(Optional.of(event));
        when(outboxEventRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        PipelineStep<CardApplicationContext> step = mockStep(true);
        when(step.execute(any())).thenThrow(new RuntimeException("permanent"));

        executor.executeStep(event.getId(), step, 0, 2);

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        assertThat(captor.getValue().getStatus()).isEqualTo(OutboxEvent.Status.FAILED);
    }

    @Test
    @DisplayName("executeStep re-throws OptimisticLockingFailureException without recording a retry")
    void executeStep_optimisticLockConflict_rethrows() throws Exception {
        OutboxEvent event = pendingEvent(0);
        when(outboxEventRepository.findById(event.getId())).thenReturn(Optional.of(event));
        when(outboxEventRepository.saveAndFlush(any()))
            .thenThrow(new OptimisticLockingFailureException("version conflict"));

        PipelineStep<CardApplicationContext> step = mockStep(true);

        assertThatThrownBy(() -> executor.executeStep(event.getId(), step, 0, 2))
            .isInstanceOf(OptimisticLockingFailureException.class);

        // No retry counter recorded — this was a concurrency skip, not a business failure
        verify(outboxEventRepository, never()).save(any());
    }

    // ── markFailed ─────────────────────────────────────────────────────────────

    @Test
    @DisplayName("markFailed sets FAILED status with reason")
    void markFailed_pendingEvent_setsFailedStatus() {
        OutboxEvent event = pendingEvent(0);
        when(outboxEventRepository.findById(event.getId())).thenReturn(Optional.of(event));
        when(outboxEventRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        executor.markFailed(event.getId(), "No steps registered");

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        assertThat(captor.getValue().getStatus()).isEqualTo(OutboxEvent.Status.FAILED);
        assertThat(captor.getValue().getLastError()).isEqualTo("No steps registered");
    }

    @Test
    @DisplayName("markFailed is a no-op when event is already PROCESSED")
    void markFailed_alreadyProcessed_doesNothing() {
        OutboxEvent event = pendingEvent(0);
        event.setStatus(OutboxEvent.Status.PROCESSED);
        when(outboxEventRepository.findById(event.getId())).thenReturn(Optional.of(event));

        executor.markFailed(event.getId(), "late failure");

        verify(outboxEventRepository, never()).save(any());
    }

    // ── helpers ────────────────────────────────────────────────────────────────

    private OutboxEvent pendingEvent(int currentStep) {
        CardApplicationContext ctx = CardApplicationContext.builder()
            .applicationId(UUID.randomUUID())
            .applicantName("Test")
            .email("t@example.com")
            .annualIncome(new BigDecimal("50000"))
            .build();

        try {
            return OutboxEvent.builder()
                .id(UUID.randomUUID())
                .correlationId(UUID.randomUUID())
                .pipelineType("CARD_APPLICATION_PIPELINE")
                .currentStep(currentStep)
                .totalSteps(2)
                .status(OutboxEvent.Status.PENDING)
                .payload(objectMapper.writeValueAsString(ctx))
                .retryCount(0)
                .build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private PipelineStep<CardApplicationContext> mockStep(boolean continueExec) throws Exception {
        PipelineStep<CardApplicationContext> step = mock(PipelineStep.class);
        when(step.getContextClass()).thenReturn(CardApplicationContext.class);
        when(step.execute(any())).thenReturn(continueExec);
        return step;
    }
}
