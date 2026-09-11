package com.demo.outbox;

import com.demo.outbox.entity.IngestionRecord;
import com.demo.outbox.entity.OutboxEvent;
import com.demo.outbox.repository.IngestionRecordRepository;
import com.demo.outbox.repository.OutboxEventRepository;
import com.demo.outbox.service.DataIngestionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the idempotency-key de-duplication actually holds against a real
 * database — the DB-level unique constraint (V3 migration) is the hard
 * guarantee; {@link DataIngestionServiceTest} only proves the Java-level
 * logic against mocks.
 */
@DisplayName("DATA_INGESTION_PIPELINE idempotency tests")
class DataIngestionIntegrationTest extends WireMockBaseTest {

    @Autowired DataIngestionService dataIngestionService;
    @Autowired IngestionRecordRepository ingestionRecordRepository;
    @Autowired OutboxEventRepository outboxEventRepository;

    @BeforeEach
    void cleanDb() {
        outboxEventRepository.deleteAll();
        ingestionRecordRepository.deleteAll();
    }

    @Test
    @DisplayName("submitting the same idempotencyKey twice returns the same record and creates only one outbox event")
    void duplicateSubmission_sameIdempotencyKey_dedupes() {
        IngestionRecord first = dataIngestionService.submitRecord("tok-abc", "+61400000000", "idem-key-1");
        IngestionRecord second = dataIngestionService.submitRecord("tok-abc", "+61400000000", "idem-key-1");

        assertThat(second.getId()).isEqualTo(first.getId());
        assertThat(ingestionRecordRepository.count()).isEqualTo(1);
        assertThat(outboxEventRepository.findByCorrelationId(first.getId())).hasSize(1);
    }

    @Test
    @DisplayName("submitting different idempotencyKeys creates independent records")
    void differentIdempotencyKeys_createIndependentRecords() {
        IngestionRecord first = dataIngestionService.submitRecord("tok-abc", "+61400000000", "idem-key-1");
        IngestionRecord second = dataIngestionService.submitRecord("tok-abc", "+61400000000", "idem-key-2");

        assertThat(second.getId()).isNotEqualTo(first.getId());
        assertThat(ingestionRecordRepository.count()).isEqualTo(2);
    }

    @Test
    @DisplayName("submitting without an idempotencyKey never dedupes, even for identical inputs")
    void noIdempotencyKey_neverDedupes() {
        dataIngestionService.submitRecord("tok-abc", "+61400000000", null);
        dataIngestionService.submitRecord("tok-abc", "+61400000000", null);

        assertThat(ingestionRecordRepository.count()).isEqualTo(2);
    }
}
