package com.demo.outbox.scheduler;

import com.demo.outbox.config.AppConfig;
import com.demo.outbox.entity.OutboxEvent;
import com.demo.outbox.repository.OutboxEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.MDC;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("PipelineOutboxPoller unit tests")
class PipelineOutboxPollerTest {

    @Mock OutboxEventRepository outboxEventRepository;
    @Mock OutboxEventProcessor outboxEventProcessor;
    @Mock AppConfig.AppProperties properties;
    @Mock AppConfig.AppProperties.Outbox outboxProps;

    @InjectMocks PipelineOutboxPoller poller;

    @BeforeEach
    void setup() {
        when(properties.getOutbox()).thenReturn(outboxProps);
        when(outboxProps.getPollBatchSize()).thenReturn(10);
    }

    private OutboxEvent pendingEvent() {
        return OutboxEvent.builder()
            .id(UUID.randomUUID())
            .correlationId(UUID.randomUUID())
            .pipelineType("CARD_APPLICATION_PIPELINE")
            .currentStep(0)
            .totalSteps(2)
            .status(OutboxEvent.Status.PENDING)
            .payload("{}")
            .retryCount(0)
            .build();
    }

    @Test
    @DisplayName("poll does nothing when no PENDING events exist")
    void poll_noPendingEvents_processorNeverCalled() {
        when(outboxEventRepository.findPendingForProcessing(10)).thenReturn(List.of());

        poller.poll();

        verify(outboxEventProcessor, never()).process(any(), any());
    }

    @Test
    @DisplayName("poll delegates one pending event to processor")
    void poll_onePendingEvent_processorCalledOnce() {
        OutboxEvent event = pendingEvent();
        when(outboxEventRepository.findPendingForProcessing(10)).thenReturn(List.of(event));

        poller.poll();

        verify(outboxEventProcessor, times(1)).process(eq(event), any());
    }

    @Test
    @DisplayName("poll delegates each event in the batch to processor")
    void poll_multiplePendingEvents_processorCalledForEach() {
        OutboxEvent e1 = pendingEvent();
        OutboxEvent e2 = pendingEvent();
        OutboxEvent e3 = pendingEvent();
        when(outboxEventRepository.findPendingForProcessing(10)).thenReturn(List.of(e1, e2, e3));

        poller.poll();

        verify(outboxEventProcessor, times(3)).process(any(), any());
    }

    @Test
    @DisplayName("poll sets MDC correlationId before delegating each event")
    void poll_mdcCorrelationIdSetDuringProcessing() {
        OutboxEvent event = pendingEvent();
        when(outboxEventRepository.findPendingForProcessing(10)).thenReturn(List.of(event));

        String[] capturedCid = {null};
        doAnswer(inv -> {
            capturedCid[0] = MDC.get("correlationId");
            return null;
        }).when(outboxEventProcessor).process(any(), any());

        poller.poll();

        assertThat(capturedCid[0]).isEqualTo(event.getCorrelationId().toString());
    }

    @Test
    @DisplayName("poll clears MDC after each event even if processor throws")
    void poll_mdcClearedAfterProcessorThrows() {
        OutboxEvent event = pendingEvent();
        when(outboxEventRepository.findPendingForProcessing(10)).thenReturn(List.of(event));
        doThrow(new RuntimeException("boom")).when(outboxEventProcessor).process(any(), any());

        assertThatThrownBy(() -> poller.poll()).isInstanceOf(RuntimeException.class);

        assertThat(MDC.get("correlationId")).isNull();
        assertThat(MDC.get("pipelineType")).isNull();
    }
}
