package com.demo.outbox;

import com.demo.outbox.repository.IngestionRecordRepository;
import com.demo.outbox.service.CsvIngestionFileProcessor;
import org.apache.sshd.common.file.virtualfs.VirtualFileSystemFactory;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.sftp.server.SftpSubsystemFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Proves the SFTP ingestion flow against a real (if in-process) SFTP server —
 * not a mock of Spring Integration's API. An embedded Apache MINA SSHD server
 * backs {@code app.sftp.remote-directory} with a real directory on disk, so
 * "uploading" a file for the test is just writing into that directory, and the
 * app's {@code SftpConfig}-wired flow does genuine SFTP listing/download/rename
 * over a real (localhost) SSH connection to reach it.
 */
@DisplayName("CSV-over-SFTP ingestion")
class CsvSftpIngestionIT extends WireMockBaseTest {

    private static final String USERNAME = "test-user";
    private static final String PASSWORD = "test-pass";

    @TempDir
    static Path sftpRoot;

    private static SshServer sshServer;

    @Autowired
    private IngestionRecordRepository ingestionRecordRepository;

    @Autowired
    private CsvIngestionFileProcessor csvIngestionFileProcessor;

    @BeforeAll
    static void startEmbeddedSftpServer() throws IOException {
        Files.createDirectories(sftpRoot.resolve("inbound"));
        Files.createDirectories(sftpRoot.resolve("archive"));
        Files.createDirectories(sftpRoot.resolve("error"));

        sshServer = SshServer.setUpDefaultServer();
        sshServer.setHost("localhost");
        sshServer.setPort(0); // random
        sshServer.setKeyPairProvider(new SimpleGeneratorHostKeyProvider());
        sshServer.setPasswordAuthenticator((username, password, session) ->
            USERNAME.equals(username) && PASSWORD.equals(password));
        sshServer.setSubsystemFactories(List.of(new SftpSubsystemFactory()));
        sshServer.setFileSystemFactory(new VirtualFileSystemFactory(sftpRoot));
        sshServer.start();
    }

    @AfterAll
    static void stopEmbeddedSftpServer() throws IOException {
        sshServer.stop();
    }

    @DynamicPropertySource
    static void sftpProperties(DynamicPropertyRegistry registry) {
        registry.add("app.sftp.enabled", () -> "true");
        registry.add("app.sftp.host", () -> "localhost");
        registry.add("app.sftp.port", () -> sshServer.getPort());
        registry.add("app.sftp.username", () -> USERNAME);
        registry.add("app.sftp.password", () -> PASSWORD);
        registry.add("app.sftp.remote-directory", () -> "/inbound");
        registry.add("app.sftp.archive-directory", () -> "/archive");
        registry.add("app.sftp.error-directory", () -> "/error");
        registry.add("app.sftp.local-directory",
            () -> sftpRoot.getParent().resolve("sftp-local-staging").toString());
        registry.add("app.sftp.poll-delay-ms", () -> "500");
    }

    @BeforeEach
    void cleanDb() {
        ingestionRecordRepository.deleteAll();
    }

    @Test
    @DisplayName("a CSV dropped in the remote inbound directory is parsed, submitted, and the remote file is archived")
    void csvFile_isIngestedAndArchived() throws Exception {
        String csv = String.join("\n",
            "token,cardholderPhone",
            "tok-AAA,+61400000001",
            "tok-BBB,+61400000002",
            "tok-CCC,"
        ) + "\n";
        Path uploaded = sftpRoot.resolve("inbound/batch-1.csv");
        Files.writeString(uploaded, csv);

        awaitArchived(sftpRoot.resolve("archive/batch-1.csv"), Duration.ofSeconds(20));

        assertThat(Files.exists(sftpRoot.resolve("inbound/batch-1.csv")))
            .as("file must no longer be in the inbound directory").isFalse();
        assertThat(Files.exists(sftpRoot.resolve("error/batch-1.csv")))
            .as("file must not have been routed to error").isFalse();

        List<String> tokens = ingestionRecordRepository.findAll().stream()
            .map(r -> r.getToken()).sorted().toList();
        assertThat(tokens).containsExactly("tok-AAA", "tok-BBB", "tok-CCC");
    }

    @Test
    @DisplayName("reprocessing the same file (e.g. a manual re-drop after an ops investigation) does not create duplicate rows")
    void reprocessingSameFile_dedupesByIdempotencyKey() throws Exception {
        // Deliberately bypasses the poller/filter layer and calls the processor
        // directly, twice, with a file of the same name and rows — that filter's
        // whole job is to stop the SFTP layer from re-offering an already-seen
        // file, so testing this "through SFTP" would just prove the filter works,
        // not that the DB-level idempotency key is the actual guard behind it.
        // This proves the guard the filter is backed by, independent of the filter.
        String csv = "token,cardholderPhone\ntok-DUP,+61400000009\n";
        String filename = "dup-1.csv";

        // Local staging copy is kept deliberately separate from the "remote" file
        // (sftpRoot/inbound/...) — that's how the real synchronizer works too:
        // downloads to a local directory distinct from the remote path it came
        // from. process() derives the remote path from the local file's *name*,
        // so both copies share a filename but live in different directories.
        Path localStagingDir = Files.createTempDirectory("local-staging");

        Path localCopy1 = localStagingDir.resolve(filename);
        Files.writeString(localCopy1, csv);
        Files.writeString(sftpRoot.resolve("inbound/" + filename), csv);
        csvIngestionFileProcessor.process(localCopy1.toFile());

        assertThat(ingestionRecordRepository.findAll()).hasSize(1);
        assertThat(Files.exists(sftpRoot.resolve("archive/" + filename))).isTrue();

        // Simulate an ops re-drop of the identical file for reprocessing.
        Path localCopy2 = localStagingDir.resolve(filename);
        Files.writeString(localCopy2, csv);
        Files.writeString(sftpRoot.resolve("inbound/" + filename), csv);
        csvIngestionFileProcessor.process(localCopy2.toFile());

        assertThat(ingestionRecordRepository.findAll())
            .as("same filename + row number => same idempotency key => no duplicate row")
            .hasSize(1);
    }

    private void awaitArchived(Path archivedPath, Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (Files.exists(archivedPath)) return;
            Thread.sleep(200);
        }
        fail("File was not archived to " + archivedPath + " within " + timeout);
    }
}
