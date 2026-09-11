package com.demo.outbox.service;

import com.demo.outbox.entity.IngestionRecord;
import com.demo.outbox.repository.IngestionRecordRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;

import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("DataIngestionService unit tests")
class DataIngestionServiceTest {

    @Mock IngestionRecordRepository ingestionRecordRepository;
    @Mock IngestionRecordWriter ingestionRecordWriter;

    @InjectMocks DataIngestionService service;

    private IngestionRecord savedRecord;

    @BeforeEach
    void setUp() {
        savedRecord = IngestionRecord.builder()
            .id(UUID.randomUUID())
            .token("tok-abc")
            .cardholderPhone("+61400000000")
            .status(IngestionRecord.Status.RECEIVED)
            .build();
    }

    @Test
    @DisplayName("submitRecord without an idempotencyKey always inserts")
    void submitRecord_noKey_alwaysInserts() {
        when(ingestionRecordWriter.insert("tok-abc", "+61400000000", null)).thenReturn(savedRecord);

        IngestionRecord result = service.submitRecord("tok-abc", "+61400000000", null);

        assertThat(result).isEqualTo(savedRecord);
        verify(ingestionRecordRepository, never()).findByIdempotencyKey(any());
        verify(ingestionRecordWriter).insert("tok-abc", "+61400000000", null);
    }

    @Test
    @DisplayName("submitRecord with a new idempotencyKey inserts once")
    void submitRecord_newKey_inserts() {
        when(ingestionRecordRepository.findByIdempotencyKey("idem-1")).thenReturn(Optional.empty());
        when(ingestionRecordWriter.insert("tok-abc", "+61400000000", "idem-1")).thenReturn(savedRecord);

        IngestionRecord result = service.submitRecord("tok-abc", "+61400000000", "idem-1");

        assertThat(result).isEqualTo(savedRecord);
        verify(ingestionRecordWriter).insert("tok-abc", "+61400000000", "idem-1");
    }

    @Test
    @DisplayName("submitRecord with a previously-seen idempotencyKey returns the existing record without inserting")
    void submitRecord_duplicateKey_returnsExistingWithoutInserting() {
        when(ingestionRecordRepository.findByIdempotencyKey("idem-1")).thenReturn(Optional.of(savedRecord));

        IngestionRecord result = service.submitRecord("tok-abc", "+61400000000", "idem-1");

        assertThat(result).isEqualTo(savedRecord);
        verify(ingestionRecordWriter, never()).insert(any(), any(), any());
    }

    @Test
    @DisplayName("submitRecord falls back to the winning record when two pods race on the same idempotencyKey")
    void submitRecord_concurrentDuplicate_returnsWinnersRecord() {
        when(ingestionRecordRepository.findByIdempotencyKey("idem-1"))
            .thenReturn(Optional.empty())   // fast-path check: not seen yet
            .thenReturn(Optional.of(savedRecord)); // fallback after the race
        when(ingestionRecordWriter.insert("tok-abc", "+61400000000", "idem-1"))
            .thenThrow(new DataIntegrityViolationException("duplicate key value violates unique constraint"));

        IngestionRecord result = service.submitRecord("tok-abc", "+61400000000", "idem-1");

        assertThat(result).isEqualTo(savedRecord);
        verify(ingestionRecordRepository, times(2)).findByIdempotencyKey("idem-1");
    }

    @Test
    @DisplayName("submitRecord rethrows a genuine constraint failure that is not an idempotencyKey collision")
    void submitRecord_noKey_constraintFailure_rethrows() {
        DataIntegrityViolationException failure = new DataIntegrityViolationException("some other constraint");
        when(ingestionRecordWriter.insert("tok-abc", "+61400000000", null)).thenThrow(failure);

        assertThatThrownBy(() -> service.submitRecord("tok-abc", "+61400000000", null))
            .isSameAs(failure);
    }

    @Test
    @DisplayName("getRecord returns entity by id")
    void getRecord_returnsEntity() {
        when(ingestionRecordRepository.findById(savedRecord.getId()))
            .thenReturn(Optional.of(savedRecord));

        IngestionRecord result = service.getRecord(savedRecord.getId());
        assertThat(result).isEqualTo(savedRecord);
    }

    @Test
    @DisplayName("getRecord throws when not found")
    void getRecord_throwsWhenNotFound() {
        UUID unknownId = UUID.randomUUID();
        when(ingestionRecordRepository.findById(unknownId)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> service.getRecord(unknownId))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Ingestion record not found");
    }
}
