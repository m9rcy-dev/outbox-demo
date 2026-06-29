package com.demo.outbox.scheduler;

import com.demo.outbox.entity.OutboxEvent;
import com.demo.outbox.pipeline.PipelineRegistry;
import com.demo.outbox.pipeline.PipelineStep;
import com.demo.outbox.pipeline.context.CardApplicationContext;
import com.demo.outbox.util.MdcContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.OptimisticLockingFailureException;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("OutboxEventProcessor — saga loop orchestration")
class OutboxEventProcessorTest {

    @Mock PipelineRegistry pipelineRegistry;
    @Mock OutboxStepExecutor stepExecutor;
    @InjectMocks OutboxEventProcessor processor;

    private MdcContext mdc;

    @BeforeEach
    void setup() {
        mdc = mock(MdcContext.class);
    }

    private OutboxEvent pendingEvent(int currentStep) {
        return OutboxEvent.builder()
            .id(UUID.randomUUID())
            .correlationId(UUID.randomUUID())
            .pipelineType("CARD_APPLICATION_PIPELINE")
            .currentStep(currentStep)
            .totalSteps(2)
            .status(OutboxEvent.Status.PENDING)
            .payload("{}")
            .retryCount(0)
            .build();
    }

    @SuppressWarnings("unchecked")
    private PipelineStep<CardApplicationContext> mockStep() {
        return mock(PipelineStep.class);
    }

    @Test
    @DisplayName("skips processing when loadAndValidate returns null")
    void process_skipsWhenEventNoLongerPending() {
        OutboxEvent staleRef = pendingEvent(0);
        when(stepExecutor.loadAndValidate(staleRef.getId())).thenReturn(null);

        processor.process(staleRef, mdc);

        verify(pipelineRegistry, never()).getSteps(any());
        verify(stepExecutor, never()).executeStep(any(), any(), anyInt(), anyInt());
    }

    @Test
    @DisplayName("calls markFailed when no steps are registered for the pipeline")
    void process_noStepsRegistered_marksEventFailed() {
        OutboxEvent event = pendingEvent(0);
        when(stepExecutor.loadAndValidate(event.getId())).thenReturn(event);
        when(pipelineRegistry.getSteps("CARD_APPLICATION_PIPELINE")).thenReturn(List.of());

        processor.process(event, mdc);

        verify(stepExecutor).markFailed(eq(event.getId()), contains("No steps found"));
        verify(stepExecutor, never()).executeStep(any(), any(), anyInt(), anyInt());
    }

    @Test
    @DisplayName("delegates each step to stepExecutor in order")
    void process_allStepsSucceed_delegatesEachStep() {
        OutboxEvent event = pendingEvent(0);
        when(stepExecutor.loadAndValidate(event.getId())).thenReturn(event);
        PipelineStep<CardApplicationContext> step0 = mockStep();
        PipelineStep<CardApplicationContext> step1 = mockStep();
        when(pipelineRegistry.getSteps("CARD_APPLICATION_PIPELINE")).thenReturn(List.of(step0, step1));
        when(stepExecutor.executeStep(eq(event.getId()), eq(step0), eq(0), eq(2))).thenReturn(true);
        when(stepExecutor.executeStep(eq(event.getId()), eq(step1), eq(1), eq(2))).thenReturn(true);

        processor.process(event, mdc);

        InOrder order = inOrder(stepExecutor);
        order.verify(stepExecutor).executeStep(event.getId(), step0, 0, 2);
        order.verify(stepExecutor).executeStep(event.getId(), step1, 1, 2);
    }

    @Test
    @DisplayName("resumes from currentStep — skips steps already committed by a prior run")
    void process_resumesFromCheckpoint() {
        OutboxEvent event = pendingEvent(1); // step 0 already committed
        when(stepExecutor.loadAndValidate(event.getId())).thenReturn(event);
        PipelineStep<CardApplicationContext> step0 = mockStep();
        PipelineStep<CardApplicationContext> step1 = mockStep();
        when(pipelineRegistry.getSteps("CARD_APPLICATION_PIPELINE")).thenReturn(List.of(step0, step1));
        when(stepExecutor.executeStep(eq(event.getId()), eq(step1), eq(1), eq(2))).thenReturn(true);

        processor.process(event, mdc);

        verify(stepExecutor, never()).executeStep(eq(event.getId()), eq(step0), eq(0), anyInt());
        verify(stepExecutor).executeStep(event.getId(), step1, 1, 2);
    }

    @Test
    @DisplayName("halts loop when executeStep returns false — step signals early exit")
    void process_stepReturnsFalse_haltsLoop() {
        OutboxEvent event = pendingEvent(0);
        when(stepExecutor.loadAndValidate(event.getId())).thenReturn(event);
        PipelineStep<CardApplicationContext> step0 = mockStep();
        PipelineStep<CardApplicationContext> step1 = mockStep();
        when(pipelineRegistry.getSteps("CARD_APPLICATION_PIPELINE")).thenReturn(List.of(step0, step1));
        when(stepExecutor.executeStep(eq(event.getId()), eq(step0), eq(0), eq(2))).thenReturn(false);

        processor.process(event, mdc);

        verify(stepExecutor).executeStep(event.getId(), step0, 0, 2);
        verify(stepExecutor, never()).executeStep(eq(event.getId()), eq(step1), anyInt(), anyInt());
    }

    @Test
    @DisplayName("sets MDC step name before delegating each step")
    void process_setsMdcStepBeforeEachExecution() {
        OutboxEvent event = pendingEvent(0);
        when(stepExecutor.loadAndValidate(event.getId())).thenReturn(event);
        PipelineStep<CardApplicationContext> step0 = mockStep();
        PipelineStep<CardApplicationContext> step1 = mockStep();
        when(pipelineRegistry.getSteps("CARD_APPLICATION_PIPELINE")).thenReturn(List.of(step0, step1));
        when(stepExecutor.executeStep(any(), any(), anyInt(), anyInt())).thenReturn(true);

        processor.process(event, mdc);

        verify(mdc, times(2)).setStep(anyString());
    }

    @Test
    @DisplayName("propagates OptimisticLockingFailureException to the caller (poll)")
    void process_propagatesOptimisticLockException() {
        OutboxEvent event = pendingEvent(0);
        when(stepExecutor.loadAndValidate(event.getId())).thenReturn(event);
        PipelineStep<CardApplicationContext> step0 = mockStep();
        when(pipelineRegistry.getSteps("CARD_APPLICATION_PIPELINE")).thenReturn(List.of(step0));
        when(stepExecutor.executeStep(any(), any(), anyInt(), anyInt()))
            .thenThrow(new OptimisticLockingFailureException("concurrent update"));

        assertThatThrownBy(() -> processor.process(event, mdc))
            .isInstanceOf(OptimisticLockingFailureException.class);
    }
}
