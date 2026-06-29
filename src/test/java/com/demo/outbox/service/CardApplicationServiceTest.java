package com.demo.outbox.service;

import com.demo.outbox.entity.CardApplication;
import com.demo.outbox.entity.OutboxEvent;
import com.demo.outbox.pipeline.context.CardApplicationContext;
import com.demo.outbox.pipeline.step.CreditBureauCheckStep;
import com.demo.outbox.repository.CardApplicationRepository;
import com.demo.outbox.repository.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("CardApplicationService unit tests")
class CardApplicationServiceTest {

    @Mock CardApplicationRepository cardApplicationRepository;
    @Mock OutboxService outboxService;

    @InjectMocks CardApplicationService service;

    private CardApplication savedApp;

    @BeforeEach
    void setUp() {
        savedApp = CardApplication.builder()
            .id(UUID.randomUUID())
            .applicantName("Jane Doe")
            .email("jane@example.com")
            .annualIncome(new BigDecimal("80000"))
            .status(CardApplication.Status.SUBMITTED)
            .build();
    }

    @Test
    @DisplayName("submitApplication saves entity and queues outbox event atomically")
    void submitApplication_savesEntityAndOutboxEvent() {
        when(cardApplicationRepository.save(any())).thenReturn(savedApp);

        CardApplication result = service.submitApplication(
            "Jane Doe", "jane@example.com", new BigDecimal("80000")
        );

        // Entity persisted
        verify(cardApplicationRepository).save(argThat(app ->
            "Jane Doe".equals(app.getApplicantName()) &&
            "jane@example.com".equals(app.getEmail()) &&
            CardApplication.Status.SUBMITTED == app.getStatus()
        ));

        // Outbox event queued for the right pipeline
        verify(outboxService).saveEvent(
            eq(CreditBureauCheckStep.PIPELINE_TYPE),
            eq(savedApp.getId()),
            argThat(ctx -> {
                CardApplicationContext c = (CardApplicationContext) ctx;
                return savedApp.getId().equals(c.getApplicationId()) &&
                       "jane@example.com".equals(c.getEmail());
            })
        );

        assertThat(result.getId()).isEqualTo(savedApp.getId());
        assertThat(result.getStatus()).isEqualTo(CardApplication.Status.SUBMITTED);
    }

    @Test
    @DisplayName("getApplication returns entity by id")
    void getApplication_returnsEntity() {
        when(cardApplicationRepository.findById(savedApp.getId()))
            .thenReturn(Optional.of(savedApp));

        CardApplication result = service.getApplication(savedApp.getId());
        assertThat(result).isEqualTo(savedApp);
    }

    @Test
    @DisplayName("getApplication throws when not found")
    void getApplication_throwsWhenNotFound() {
        UUID unknownId = UUID.randomUUID();
        when(cardApplicationRepository.findById(unknownId)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> service.getApplication(unknownId))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Application not found");
    }
}
