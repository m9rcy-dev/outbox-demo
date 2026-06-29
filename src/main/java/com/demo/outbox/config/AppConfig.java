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
    }
}
