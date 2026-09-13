package com.demo.outbox.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.client.RestTemplate;

import lombok.Data;

@Configuration
@EnableScheduling
@EnableConfigurationProperties(AppConfig.AppProperties.class)
public class AppConfig {

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }

    @Data
    @ConfigurationProperties(prefix = "app")
    public static class AppProperties {
        private Api api = new Api();
        private Outbox outbox = new Outbox();
        private Sftp sftp = new Sftp();

        @Data
        public static class Api {
            private String creditBureauUrl;
            private String cardProviderUrl;
            private String notificationUrl;
        }

        @Data
        public static class Outbox {
            private int pollBatchSize = 10;
            private int maxRetries = 3;
            private long pollDelayMs = 5000;
        }

        @Data
        public static class Sftp {
            /** Master switch — SftpConfig only activates when this is true. */
            private boolean enabled = false;
            private String host;
            private int port = 22;
            private String username;
            private String password;
            /** Dev-friendly default; production should supply a known_hosts file instead. */
            private boolean strictHostKeyChecking = false;
            private String remoteDirectory = "/inbound";
            /** Rename destination once every row is durably in an outbox_event row. */
            private String archiveDirectory = "/archive";
            /** Rename destination when a file fails to parse/submit. */
            private String errorDirectory = "/error";
            /** Local staging directory — never the source of truth, deleted after every attempt. */
            private String localDirectory = "/tmp/outbox-demo/sftp-inbound";
            private String filenamePattern = "*.csv";
            private long pollDelayMs = 5000;
        }
    }
}
