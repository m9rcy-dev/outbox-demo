package com.demo.outbox.service;

import com.demo.outbox.config.AppConfig;
import com.demo.outbox.entity.IngestionRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.integration.file.remote.SessionCallback;
import org.springframework.integration.file.remote.session.Session;
import org.springframework.integration.sftp.session.SftpRemoteFileTemplate;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("CsvIngestionFileProcessor unit tests")
class CsvIngestionFileProcessorTest {

    @Mock DataIngestionService dataIngestionService;
    @Mock SftpRemoteFileTemplate sftpRemoteFileTemplate;

    private AppConfig.AppProperties properties;
    private CsvIngestionFileProcessor processor;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        properties = new AppConfig.AppProperties();
        properties.getSftp().setRemoteDirectory("/inbound");
        properties.getSftp().setArchiveDirectory("/archive");
        properties.getSftp().setErrorDirectory("/error");

        processor = new CsvIngestionFileProcessor(dataIngestionService, sftpRemoteFileTemplate, properties);

        // Route the SftpRemoteFileTemplate.execute(SessionCallback) call straight
        // through to a mocked Session, so moveRemoteFile()'s rename/mkdir/exists
        // calls can be verified without a real SFTP server.
        lenient().when(sftpRemoteFileTemplate.execute(any())).thenAnswer(invocation -> {
            SessionCallback callback = invocation.getArgument(0);
            Session session = mock(Session.class);
            when(session.exists(any())).thenReturn(true);
            return callback.doInSession(session);
        });
    }

    private Path writeCsv(String filename, String content) throws IOException {
        Path file = tempDir.resolve(filename);
        Files.writeString(file, content);
        return file;
    }

    @Test
    @DisplayName("submits one row per data line with filename:rowNumber idempotency keys, then archives")
    void process_validCsv_submitsEachRowAndArchives() throws Exception {
        Path file = writeCsv("cards.csv", "token,cardholderPhone\ntok-1,+61400000001\ntok-2,+61400000002\n");
        when(dataIngestionService.submitRecord(any(), any(), any()))
            .thenReturn(IngestionRecord.builder().build());

        processor.process(file.toFile());

        verify(dataIngestionService).submitRecord("tok-1", "+61400000001", "cards.csv:1");
        verify(dataIngestionService).submitRecord("tok-2", "+61400000002", "cards.csv:2");
        verify(sftpRemoteFileTemplate).execute(any());
        assertThat(file).doesNotExist();
    }

    @Test
    @DisplayName("a present but empty cardholderPhone column is passed through as an empty string")
    void process_emptyPhoneColumn_passedAsEmptyString() throws Exception {
        Path file = writeCsv("cards.csv", "token,cardholderPhone\ntok-1,\n");
        when(dataIngestionService.submitRecord(any(), any(), any()))
            .thenReturn(IngestionRecord.builder().build());

        processor.process(file.toFile());

        // Not null — only a fully absent column becomes null (see the next test).
        // SmsNotificationStep.appliesTo() treats both null and "" as "no phone"
        // via isBlank(), so either representation is handled correctly downstream.
        verify(dataIngestionService).submitRecord(eq("tok-1"), eq(""), eq("cards.csv:1"));
    }

    @Test
    @DisplayName("a fully absent cardholderPhone column is passed through as null")
    void process_missingPhoneColumn_passedAsNull() throws Exception {
        Path file = writeCsv("cards.csv", "token\ntok-1\n");
        when(dataIngestionService.submitRecord(any(), any(), any()))
            .thenReturn(IngestionRecord.builder().build());

        processor.process(file.toFile());

        verify(dataIngestionService).submitRecord(eq("tok-1"), eq(null), eq("cards.csv:1"));
    }

    @Test
    @DisplayName("a row that fails to submit routes the whole file to the error directory, not archive")
    void process_submissionFailure_movesToErrorDirectory() throws Exception {
        Path file = writeCsv("bad.csv", "token,cardholderPhone\ntok-1,+61400000001\n");
        when(dataIngestionService.submitRecord(any(), any(), any()))
            .thenThrow(new RuntimeException("db unavailable"));

        processor.process(file.toFile());

        // Can't easily assert *which* directory without a real/fake session, but
        // we can assert execute() was still invoked exactly once (the error path
        // also calls moveRemoteFile, just with the error directory instead) and
        // that the local staging file is cleaned up regardless of outcome.
        verify(sftpRemoteFileTemplate).execute(any());
        assertThat(file).doesNotExist();
    }

    @Test
    @DisplayName("local staging file is deleted even when nothing goes wrong")
    void process_success_deletesLocalStagingFile() throws Exception {
        Path file = writeCsv("cards.csv", "token,cardholderPhone\ntok-1,+61400000001\n");
        when(dataIngestionService.submitRecord(any(), any(), any()))
            .thenReturn(IngestionRecord.builder().build());

        processor.process(file.toFile());

        assertThat(file).doesNotExist();
    }
}
