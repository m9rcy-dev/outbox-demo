package com.demo.outbox.service;

import com.demo.outbox.config.AppConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.integration.sftp.session.SftpRemoteFileTemplate;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;

/**
 * Turns one downloaded CSV file into DATA_INGESTION_PIPELINE submissions.
 *
 * Expected columns: {@code token}, {@code cardholderPhone} (matches
 * {@link DataIngestionService#submitRecord}'s inputs). Each row's idempotency key
 * is {@code <filename>:<row number>} — stable across retries of the same file
 * (e.g. if this pod dies mid-file, whichever pod re-parses it produces the same
 * keys, so already-submitted rows are silently deduped rather than resubmitted).
 *
 * The remote file is the source of truth, not the local staging copy: the local
 * file is deleted after every attempt regardless of outcome, while the remote
 * file is archived only on success or moved to an error folder on failure, so a
 * human can find and re-drive it.
 */
@Service
@ConditionalOnProperty(prefix = "app.sftp", name = "enabled", havingValue = "true")
@RequiredArgsConstructor
@Slf4j
public class CsvIngestionFileProcessor {

    private final DataIngestionService dataIngestionService;
    private final SftpRemoteFileTemplate sftpRemoteFileTemplate;
    private final AppConfig.AppProperties properties;

    public void process(File localFile) {
        String filename = localFile.getName();
        int rowsSubmitted = 0;

        try (Reader reader = new FileReader(localFile);
             CSVParser parser = CSVFormat.DEFAULT.builder()
                 .setHeader()
                 .setSkipHeaderRecord(true)
                 .setTrim(true)
                 .get()
                 .parse(reader)) {

            for (CSVRecord record : parser) {
                rowsSubmitted++;
                String token = record.get("token");
                String cardholderPhone = record.isSet("cardholderPhone") ? record.get("cardholderPhone") : null;
                String idempotencyKey = filename + ":" + rowsSubmitted;

                dataIngestionService.submitRecord(token, cardholderPhone, idempotencyKey);
            }

            moveRemoteFile(filename, properties.getSftp().getArchiveDirectory());
            log.info("Ingested file {} — {} row(s) submitted, archived", filename, rowsSubmitted);

        } catch (Exception e) {
            log.error("Failed to ingest file {} after {} row(s) — moving to error directory for investigation",
                filename, rowsSubmitted, e);
            moveRemoteFile(filename, properties.getSftp().getErrorDirectory());
        } finally {
            // Never the source of truth — safe to discard regardless of outcome.
            if (!localFile.delete()) {
                log.warn("Could not delete local staging file {}", localFile.getAbsolutePath());
            }
        }
    }

    private void moveRemoteFile(String filename, String destinationDirectory) {
        String from = properties.getSftp().getRemoteDirectory() + "/" + filename;
        String to = destinationDirectory + "/" + filename;
        sftpRemoteFileTemplate.execute(session -> {
            if (!session.exists(destinationDirectory)) {
                session.mkdir(destinationDirectory);
            }
            session.rename(from, to);
            return null;
        });
    }
}
