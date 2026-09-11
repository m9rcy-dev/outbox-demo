package com.demo.outbox.pipeline.step.ingestion;

import com.demo.outbox.entity.IngestionRecord;
import com.demo.outbox.pipeline.StepMode;
import com.demo.outbox.pipeline.context.DataIngestionContext;
import com.demo.outbox.repository.IngestionRecordRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("DATA_INGESTION_PIPELINE step unit tests")
class IngestionPipelineStepTest {

    private IngestionRecord record;
    private DataIngestionContext ctx;
    private UUID recordId;

    @BeforeEach
    void base() {
        recordId = UUID.randomUUID();
        record = IngestionRecord.builder()
            .id(recordId)
            .token("tok-abc")
            .cardholderPhone("+61400000000")
            .status(IngestionRecord.Status.RECEIVED)
            .build();

        ctx = DataIngestionContext.builder()
            .recordId(recordId)
            .token("tok-abc")
            .cardholderPhone("+61400000000")
            .build();
    }

    // ── Step 0 ────────────────────────────────────────────────────────────────
    @Nested
    @DisplayName("DetokenisationStep")
    class DetokenisationStepTest {

        @Mock IngestionRecordRepository ingestionRecordRepository;
        @InjectMocks DetokenisationStep step;

        @Test
        @DisplayName("is BLOCKING_REQUIRED with no maxRetries override")
        void mode() {
            assertThat(step.getMode()).isEqualTo(StepMode.BLOCKING_REQUIRED);
            assertThat(step.getMaxRetries()).isEqualTo(-1);
            assertThat(step.getStepIndex()).isZero();
            assertThat(step.getPipelineType()).isEqualTo(DetokenisationStep.PIPELINE_TYPE);
        }

        @Test
        @DisplayName("sets panLast4 on context and updates entity")
        void execute_setsPanLast4() throws Exception {
            when(ingestionRecordRepository.findById(recordId)).thenReturn(Optional.of(record));
            when(ingestionRecordRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

            boolean result = step.execute(ctx);

            assertThat(result).isTrue();
            assertThat(ctx.getPanLast4()).hasSize(4);
            assertThat(record.getStatus()).isEqualTo(IngestionRecord.Status.DETOKENISED);
        }

        @Test
        @DisplayName("propagates exception when record is missing")
        void execute_throwsWhenRecordMissing() {
            when(ingestionRecordRepository.findById(recordId)).thenReturn(Optional.empty());

            org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                () -> step.execute(ctx));
        }
    }

    // ── Step 1 ────────────────────────────────────────────────────────────────
    @Nested
    @DisplayName("MastercardSearchStep")
    class MastercardSearchStepTest {

        @Mock IngestionRecordRepository ingestionRecordRepository;
        @InjectMocks MastercardSearchStep step;

        @BeforeEach
        void setPan() { ctx.setPanLast4("1111"); }

        @Test
        @DisplayName("is BLOCKING_SELF_RETRIED with a thin maxRetries override")
        void mode() {
            assertThat(step.getMode()).isEqualTo(StepMode.BLOCKING_SELF_RETRIED);
            assertThat(step.getMaxRetries()).isEqualTo(1);
            assertThat(step.getStepIndex()).isEqualTo(1);
        }

        @Test
        @DisplayName("sets mastercardMatchId on context and updates entity")
        void execute_setsMatchId() throws Exception {
            when(ingestionRecordRepository.findById(recordId)).thenReturn(Optional.of(record));
            when(ingestionRecordRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

            boolean result = step.execute(ctx);

            assertThat(result).isTrue();
            assertThat(ctx.getMastercardMatchId()).isNotBlank();
            assertThat(record.getStatus()).isEqualTo(IngestionRecord.Status.MASTERCARD_MATCHED);
        }
    }

    // ── Step 2 ────────────────────────────────────────────────────────────────
    @Nested
    @DisplayName("InternalNotesStep")
    class InternalNotesStepTest {

        InternalNotesStep step = new InternalNotesStep();

        @BeforeEach
        void setMatch() { ctx.setMastercardMatchId("MC-MATCH-1111"); }

        @Test
        @DisplayName("is FIRE_AND_FORGET")
        void mode() {
            assertThat(step.getMode()).isEqualTo(StepMode.FIRE_AND_FORGET);
            assertThat(step.getStepIndex()).isEqualTo(2);
        }

        @Test
        @DisplayName("sets notesId on context")
        void execute_setsNotesId() throws Exception {
            boolean result = step.execute(ctx);

            assertThat(result).isTrue();
            assertThat(ctx.getNotesId()).isNotBlank();
        }
    }

    // ── Step 3 ────────────────────────────────────────────────────────────────
    @Nested
    @DisplayName("SmsNotificationStep")
    class SmsNotificationStepTest {

        SmsNotificationStep step = new SmsNotificationStep();

        @Test
        @DisplayName("is FIRE_AND_FORGET")
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
            ctx.setCardholderPhone("+61400000000");
            assertThat(step.appliesTo(ctx)).isTrue();
        }

        @Test
        @DisplayName("appliesTo is false when there is no cardholder phone")
        void appliesTo_noPhone_false() {
            ctx.setCardholderPhone(null);
            assertThat(step.appliesTo(ctx)).isFalse();

            ctx.setCardholderPhone("   ");
            assertThat(step.appliesTo(ctx)).isFalse();
        }
    }

    // ── Step 4 ────────────────────────────────────────────────────────────────
    @Nested
    @DisplayName("MqPublishStep")
    class MqPublishStepTest {

        @Mock IngestionRecordRepository ingestionRecordRepository;
        @InjectMocks MqPublishStep step;

        @Test
        @DisplayName("is BLOCKING_SELF_RETRIED with maxRetries=2")
        void mode() {
            assertThat(step.getMode()).isEqualTo(StepMode.BLOCKING_SELF_RETRIED);
            assertThat(step.getMaxRetries()).isEqualTo(2);
            assertThat(step.getStepIndex()).isEqualTo(4);
        }

        @Test
        @DisplayName("sets messageId on context and marks record INGESTED")
        void execute_publishesAndMarksIngested() throws Exception {
            when(ingestionRecordRepository.findById(recordId)).thenReturn(Optional.of(record));
            when(ingestionRecordRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

            boolean result = step.execute(ctx);

            assertThat(result).isTrue();
            assertThat(ctx.getMessageId()).isNotBlank();
            assertThat(record.getStatus()).isEqualTo(IngestionRecord.Status.INGESTED);
        }
    }
}
