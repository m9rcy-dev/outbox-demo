package com.demo.outbox.pipeline.step;

import com.demo.outbox.api.CardProviderClient;
import com.demo.outbox.api.CreditBureauClient;
import com.demo.outbox.api.NotificationClient;
import com.demo.outbox.entity.CardApplication;
import com.demo.outbox.pipeline.context.CardApplicationContext;
import com.demo.outbox.repository.CardApplicationRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("Pipeline step unit tests")
class PipelineStepTest {

    private CardApplication app;
    private CardApplicationContext ctx;
    private UUID appId;

    @BeforeEach
    void base() {
        appId = UUID.randomUUID();
        app = CardApplication.builder()
            .id(appId)
            .applicantName("Alice")
            .email("alice@example.com")
            .annualIncome(new BigDecimal("90000"))
            .status(CardApplication.Status.SUBMITTED)
            .build();

        ctx = CardApplicationContext.builder()
            .applicationId(appId)
            .applicantName("Alice")
            .email("alice@example.com")
            .annualIncome(new BigDecimal("90000"))
            .build();
    }

    // ── Step 0 ────────────────────────────────────────────────────────────────
    @Nested
    @DisplayName("CreditBureauCheckStep")
    class CreditBureauCheckStepTest {

        @Mock CreditBureauClient creditBureauClient;
        @Mock CardApplicationRepository cardApplicationRepository;
        @InjectMocks CreditBureauCheckStep step;

        @Test
        @DisplayName("sets creditScore on context and updates entity")
        void execute_setsScoreAndUpdatesEntity() throws Exception {
            when(creditBureauClient.checkCredit("Alice", new BigDecimal("90000"))).thenReturn(720);
            when(cardApplicationRepository.findById(appId)).thenReturn(Optional.of(app));
            when(cardApplicationRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

            boolean result = step.execute(ctx);

            assertThat(result).isTrue();
            assertThat(ctx.getCreditScore()).isEqualTo(720);
            assertThat(app.getCreditScore()).isEqualTo(720);
            assertThat(app.getStatus()).isEqualTo(CardApplication.Status.CREDIT_CHECKED);
        }

        @Test
        @DisplayName("propagates exception on API failure")
        void execute_propagatesExceptionOnApiFailure() {
            when(creditBureauClient.checkCredit(any(), any()))
                .thenThrow(new RuntimeException("bureau down"));

            assertThatThrownBy(() -> step.execute(ctx))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("bureau down");
        }

        @Test
        @DisplayName("getPipelineType, getStepIndex, and getContextClass return correct values")
        void metadata() {
            assertThat(step.getPipelineType()).isEqualTo(CreditBureauCheckStep.PIPELINE_TYPE);
            assertThat(step.getStepIndex()).isZero();
            assertThat(step.getContextClass()).isEqualTo(CardApplicationContext.class);
        }

        @Test
        @DisplayName("throws IllegalStateException when CardApplication not found")
        void execute_throwsWhenApplicationNotFound() {
            when(creditBureauClient.checkCredit(any(), any())).thenReturn(750);
            when(cardApplicationRepository.findById(appId)).thenReturn(Optional.empty());

            assertThatThrownBy(() -> step.execute(ctx))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("CardApplication not found");
        }
    }

    // ── Step 1 ────────────────────────────────────────────────────────────────
    @Nested
    @DisplayName("CardProviderRegisterStep")
    class CardProviderRegisterStepTest {

        @Mock CardProviderClient cardProviderClient;
        @Mock CardApplicationRepository cardApplicationRepository;
        @InjectMocks CardProviderRegisterStep step;

        @BeforeEach
        void setScore() { ctx.setCreditScore(720); }

        @Test
        @DisplayName("sets providerRef on context and updates entity")
        void execute_setsProviderRef() throws Exception {
            when(cardProviderClient.registerApplication(appId, "Alice", 720))
                .thenReturn("PROV-XYZ");
            when(cardApplicationRepository.findById(appId)).thenReturn(Optional.of(app));
            when(cardApplicationRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

            boolean result = step.execute(ctx);

            assertThat(result).isTrue();
            assertThat(ctx.getProviderRef()).isEqualTo("PROV-XYZ");
            assertThat(app.getProviderRef()).isEqualTo("PROV-XYZ");
            assertThat(app.getStatus()).isEqualTo(CardApplication.Status.PROVIDER_REGISTERED);
        }

        @Test
        @DisplayName("step index is 1 and getContextClass returns correct type")
        void metadata() {
            assertThat(step.getStepIndex()).isEqualTo(1);
            assertThat(step.getContextClass()).isEqualTo(CardApplicationContext.class);
        }

        @Test
        @DisplayName("throws IllegalStateException when CardApplication not found")
        void execute_throwsWhenApplicationNotFound() throws Exception {
            when(cardProviderClient.registerApplication(any(), any(), anyInt())).thenReturn("REF");
            when(cardApplicationRepository.findById(appId)).thenReturn(Optional.empty());

            assertThatThrownBy(() -> step.execute(ctx))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("CardApplication not found");
        }
    }

    // ── Step 2 ────────────────────────────────────────────────────────────────
    @Nested
    @DisplayName("NotificationStep")
    class NotificationStepTest {

        @Mock NotificationClient notificationClient;
        @Mock CardApplicationRepository cardApplicationRepository;
        @InjectMocks NotificationStep step;

        @BeforeEach
        void setProviderRef() { ctx.setProviderRef("PROV-XYZ"); }

        @Test
        @DisplayName("sets notificationId on context and updates entity")
        void execute_setsNotificationId() throws Exception {
            when(notificationClient.sendApprovalNotification("alice@example.com", "Alice", "PROV-XYZ"))
                .thenReturn("NOTIF-ABC");
            when(cardApplicationRepository.findById(appId)).thenReturn(Optional.of(app));
            when(cardApplicationRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

            boolean result = step.execute(ctx);

            assertThat(result).isTrue();
            assertThat(ctx.getNotificationId()).isEqualTo("NOTIF-ABC");
            assertThat(app.getNotificationId()).isEqualTo("NOTIF-ABC");
            assertThat(app.getStatus()).isEqualTo(CardApplication.Status.NOTIFIED);
        }

        @Test
        @DisplayName("step index is 2 and getContextClass returns correct type")
        void metadata() {
            assertThat(step.getStepIndex()).isEqualTo(2);
            assertThat(step.getContextClass()).isEqualTo(CardApplicationContext.class);
        }

        @Test
        @DisplayName("throws IllegalStateException when CardApplication not found")
        void execute_throwsWhenApplicationNotFound() throws Exception {
            when(notificationClient.sendApprovalNotification(any(), any(), any())).thenReturn("N-1");
            when(cardApplicationRepository.findById(appId)).thenReturn(Optional.empty());

            assertThatThrownBy(() -> step.execute(ctx))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("CardApplication not found");
        }
    }

    // ── Step 3 ────────────────────────────────────────────────────────────────
    @Nested
    @DisplayName("FinalDbUpdateStep")
    class FinalDbUpdateStepTest {

        @Mock CardApplicationRepository cardApplicationRepository;
        @InjectMocks FinalDbUpdateStep step;

        @BeforeEach
        void setAllFields() {
            ctx.setCreditScore(720);
            ctx.setProviderRef("PROV-XYZ");
            ctx.setNotificationId("NOTIF-ABC");
        }

        @Test
        @DisplayName("marks application COMPLETED")
        void execute_marksCompleted() throws Exception {
            when(cardApplicationRepository.findById(appId)).thenReturn(Optional.of(app));
            when(cardApplicationRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

            boolean result = step.execute(ctx);

            assertThat(result).isTrue();
            assertThat(app.getStatus()).isEqualTo(CardApplication.Status.COMPLETED);
        }

        @Test
        @DisplayName("step index is 3 and getContextClass returns correct type")
        void metadata() {
            assertThat(step.getStepIndex()).isEqualTo(3);
            assertThat(step.getContextClass()).isEqualTo(CardApplicationContext.class);
        }

        @Test
        @DisplayName("throws IllegalStateException when CardApplication not found")
        void execute_throwsWhenApplicationNotFound() {
            when(cardApplicationRepository.findById(appId)).thenReturn(Optional.empty());

            assertThatThrownBy(() -> step.execute(ctx))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("CardApplication not found");
        }
    }
}
