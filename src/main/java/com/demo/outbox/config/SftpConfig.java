package com.demo.outbox.config;

import com.demo.outbox.service.CsvIngestionFileProcessor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.file.filters.CompositeFileListFilter;
import org.springframework.integration.file.remote.session.CachingSessionFactory;
import org.springframework.integration.file.remote.session.SessionFactory;
import org.springframework.integration.metadata.SimpleMetadataStore;
import org.springframework.integration.sftp.filters.SftpPersistentAcceptOnceFileListFilter;
import org.springframework.integration.sftp.filters.SftpSimplePatternFileListFilter;
import org.springframework.integration.sftp.inbound.SftpInboundFileSynchronizer;
import org.springframework.integration.sftp.inbound.SftpInboundFileSynchronizingMessageSource;
import org.springframework.integration.sftp.session.DefaultSftpSessionFactory;
import org.springframework.integration.sftp.session.SftpRemoteFileTemplate;

import java.io.File;

/**
 * Watches a remote SFTP directory for {@code *.csv} files and feeds each one into
 * {@link CsvIngestionFileProcessor}, which submits every row to the
 * DATA_INGESTION_PIPELINE and archives the remote file once submission is durable.
 *
 * Only active when {@code app.sftp.enabled=true} — most environments (including
 * every test in this codebase except the ones that specifically exercise this flow)
 * have no SFTP server to talk to.
 *
 * <h2>Multi-pod note</h2>
 * The {@link SftpPersistentAcceptOnceFileListFilter} here uses an in-memory,
 * per-pod {@link SimpleMetadataStore} — it only stops THIS pod re-downloading a
 * file it has already seen. It deliberately does NOT attempt cross-pod
 * coordination (a shared/distributed metadata store, a distributed lock, leader
 * election) because none of that is the actual correctness guard here: if two
 * pods both see a brand-new file in the same poll window, both will parse it and
 * both will call {@link com.demo.outbox.service.DataIngestionService#submitRecord}
 * for the same rows — and the idempotency-key unique constraint already built for
 * that service (keyed on {@code filename:rowNumber}) is what actually prevents the
 * duplicate, the same way it does for a duplicate HTTP submission. This SFTP layer
 * inherits that guarantee for free instead of needing its own distributed-locking
 * story.
 */
@Configuration
@ConditionalOnProperty(prefix = "app.sftp", name = "enabled", havingValue = "true")
public class SftpConfig {

    @Bean
    public SessionFactory<org.apache.sshd.sftp.client.SftpClient.DirEntry> sftpSessionFactory(
            AppConfig.AppProperties properties) {
        AppConfig.AppProperties.Sftp sftp = properties.getSftp();

        DefaultSftpSessionFactory factory = new DefaultSftpSessionFactory(true);
        factory.setHost(sftp.getHost());
        factory.setPort(sftp.getPort());
        factory.setUser(sftp.getUsername());
        factory.setPassword(sftp.getPassword());
        factory.setAllowUnknownKeys(!sftp.isStrictHostKeyChecking());
        return new CachingSessionFactory<>(factory);
    }

    @Bean
    public SftpInboundFileSynchronizer sftpInboundFileSynchronizer(
            SessionFactory<org.apache.sshd.sftp.client.SftpClient.DirEntry> sftpSessionFactory,
            AppConfig.AppProperties properties) {
        AppConfig.AppProperties.Sftp sftp = properties.getSftp();

        SftpInboundFileSynchronizer synchronizer = new SftpInboundFileSynchronizer(sftpSessionFactory);
        synchronizer.setRemoteDirectory(sftp.getRemoteDirectory());
        synchronizer.setPreserveTimestamp(true);
        // Never delete-on-download here — CsvIngestionFileProcessor archives the
        // remote file explicitly, only after every row is durably submitted.
        synchronizer.setDeleteRemoteFiles(false);

        CompositeFileListFilter<org.apache.sshd.sftp.client.SftpClient.DirEntry> filter =
            new CompositeFileListFilter<>();
        filter.addFilter(new SftpSimplePatternFileListFilter(sftp.getFilenamePattern()));
        // In-memory / per-pod on purpose — see class Javadoc.
        filter.addFilter(new SftpPersistentAcceptOnceFileListFilter(new SimpleMetadataStore(), "sftp-ingestion-"));
        synchronizer.setFilter(filter);

        return synchronizer;
    }

    @Bean
    public SftpInboundFileSynchronizingMessageSource sftpMessageSource(
            SftpInboundFileSynchronizer sftpInboundFileSynchronizer,
            AppConfig.AppProperties properties) {
        SftpInboundFileSynchronizingMessageSource source =
            new SftpInboundFileSynchronizingMessageSource(sftpInboundFileSynchronizer);
        source.setLocalDirectory(new File(properties.getSftp().getLocalDirectory()));
        source.setAutoCreateLocalDirectory(true);
        return source;
    }

    /** Used by {@link CsvIngestionFileProcessor} to archive/error-move the remote file. */
    @Bean
    public SftpRemoteFileTemplate sftpRemoteFileTemplate(
            SessionFactory<org.apache.sshd.sftp.client.SftpClient.DirEntry> sftpSessionFactory) {
        return new SftpRemoteFileTemplate(sftpSessionFactory);
    }

    @Bean
    public IntegrationFlow sftpIngestionFlow(
            SftpInboundFileSynchronizingMessageSource sftpMessageSource,
            CsvIngestionFileProcessor csvIngestionFileProcessor,
            AppConfig.AppProperties properties) {
        return IntegrationFlow
            .from(sftpMessageSource,
                c -> c.poller(Pollers.fixedDelay(properties.getSftp().getPollDelayMs())))
            .handle(message -> csvIngestionFileProcessor.process((File) message.getPayload()))
            .get();
    }
}
