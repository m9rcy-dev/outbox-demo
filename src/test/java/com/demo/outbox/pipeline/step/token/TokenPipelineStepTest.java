package com.demo.outbox.pipeline.step.token;

import com.demo.outbox.pipeline.StepMode;
import com.demo.outbox.pipeline.context.TokenUpdateContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("TOKEN_UPDATE_PIPELINE step unit tests")
class TokenPipelineStepTest {

    private TokenUpdateContext ctx;

    @BeforeEach
    void base() {
        ctx = TokenUpdateContext.builder()
            .cardId(UUID.randomUUID())
            .tokenId("TOK-123")
            .desiredStatus("SUSPENDED")
            .cardholderPhone("+61400000000")
            .build();
    }

    @Nested
    @DisplayName("MastercardTokenUpdateStep")
    class MastercardTokenUpdateStepTest {

        MastercardTokenUpdateStep step = new MastercardTokenUpdateStep();

        @Test
        @DisplayName("is BLOCKING_SELF_RETRIED with maxRetries=1 — must succeed, client already retried")
        void mode() {
            assertThat(step.getMode()).isEqualTo(StepMode.BLOCKING_SELF_RETRIED);
            assertThat(step.getMaxRetries()).isEqualTo(1);
            assertThat(step.getStepIndex()).isZero();
            assertThat(step.getPipelineType()).isEqualTo(MastercardTokenUpdateStep.PIPELINE_TYPE);
        }

        @Test
        @DisplayName("sets mastercardRef and updatedStatus deterministically for the same inputs")
        void execute_setsMastercardRefIdempotently() throws Exception {
            boolean result = step.execute(ctx);

            assertThat(result).isTrue();
            assertThat(ctx.getUpdatedStatus()).isEqualTo("SUSPENDED");
            String firstRef = ctx.getMastercardRef();
            assertThat(firstRef).isNotBlank();

            // Same context (same cardId/tokenId/desiredStatus) → same idempotency key,
            // as required for a retry to be safe against double-applying on Mastercard's side.
            ctx.setMastercardRef(null);
            step.execute(ctx);
            assertThat(ctx.getMastercardRef()).isEqualTo(firstRef);
        }
    }

    @Nested
    @DisplayName("LocalCardStatusSyncStep")
    class LocalCardStatusSyncStepTest {

        LocalCardStatusSyncStep step = new LocalCardStatusSyncStep();

        @BeforeEach
        void setUpdatedStatus() { ctx.setUpdatedStatus("SUSPENDED"); ctx.setMastercardRef("MC-REF-1"); }

        @Test
        @DisplayName("is BLOCKING_REQUIRED (default) — no override, keeps retrying against the global ceiling")
        void mode() {
            assertThat(step.getMode()).isEqualTo(StepMode.BLOCKING_REQUIRED);
            assertThat(step.getMaxRetries()).isEqualTo(-1);
            assertThat(step.getStepIndex()).isEqualTo(1);
        }

        @Test
        @DisplayName("executes without throwing")
        void execute_succeeds() throws Exception {
            assertThat(step.execute(ctx)).isTrue();
        }
    }

    @Nested
    @DisplayName("MqPublishStep")
    class MqPublishStepTest {

        MqPublishStep step = new MqPublishStep();

        @Test
        @DisplayName("is BLOCKING_SELF_RETRIED with maxRetries=2")
        void mode() {
            assertThat(step.getMode()).isEqualTo(StepMode.BLOCKING_SELF_RETRIED);
            assertThat(step.getMaxRetries()).isEqualTo(2);
            assertThat(step.getStepIndex()).isEqualTo(2);
        }

        @Test
        @DisplayName("sets messageId on context")
        void execute_setsMessageId() throws Exception {
            boolean result = step.execute(ctx);

            assertThat(result).isTrue();
            assertThat(ctx.getMessageId()).isNotBlank();
        }
    }

    @Nested
    @DisplayName("SmsNotificationStep")
    class SmsNotificationStepTest {

        SmsNotificationStep step = new SmsNotificationStep();

        @Test
        @DisplayName("is FIRE_AND_FORGET — last step, best-effort courtesy notification")
        void mode() {
            assertThat(step.getMode()).isEqualTo(StepMode.FIRE_AND_FORGET);
            assertThat(step.getStepIndex()).isEqualTo(3);
        }

        @Test
        @DisplayName("sets smsMessageId on context")
        void execute_setsSmsMessageId() throws Exception {
            boolean result = step.execute(ctx);

            assertThat(result).isTrue();
            assertThat(ctx.getSmsMessageId()).isNotBlank();
        }

        @Test
        @DisplayName("appliesTo is true when a cardholder phone is present")
        void appliesTo_phonePresent_true() {
            assertThat(step.appliesTo(ctx)).isTrue(); // base() sets a phone number
        }

        @Test
        @DisplayName("appliesTo is false when there is no cardholder phone (e.g. a corporate card)")
        void appliesTo_noPhone_false() {
            ctx.setCardholderPhone(null);
            assertThat(step.appliesTo(ctx)).isFalse();
        }
    }
}
