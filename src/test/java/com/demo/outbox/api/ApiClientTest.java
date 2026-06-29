package com.demo.outbox.api;

import com.demo.outbox.config.AppConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("API client unit tests")
class ApiClientTest {

    @Mock RestTemplate restTemplate;
    @Mock AppConfig.AppProperties properties;
    @Mock AppConfig.AppProperties.Api api;

    @BeforeEach
    void setup() {
        when(properties.getApi()).thenReturn(api);
    }

    @Nested
    @DisplayName("CreditBureauClient")
    class CreditBureauClientTests {

        CreditBureauClient client;

        @BeforeEach
        void init() {
            when(api.getCreditBureauUrl()).thenReturn("http://credit");
            client = new CreditBureauClient(restTemplate, properties);
        }

        @Test
        @DisplayName("null response throws IllegalStateException")
        void checkCredit_nullResponse_throws() {
            when(restTemplate.postForObject(anyString(), any(), eq(Map.class))).thenReturn(null);

            assertThatThrownBy(() -> client.checkCredit("Alice", BigDecimal.TEN))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("invalid response");
        }

        @Test
        @DisplayName("response missing creditScore key throws IllegalStateException")
        void checkCredit_missingKey_throws() {
            when(restTemplate.postForObject(anyString(), any(), eq(Map.class)))
                .thenReturn(Map.of("other", "value"));

            assertThatThrownBy(() -> client.checkCredit("Alice", BigDecimal.TEN))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("invalid response");
        }
    }

    @Nested
    @DisplayName("CardProviderClient")
    class CardProviderClientTests {

        CardProviderClient client;

        @BeforeEach
        void init() {
            when(api.getCardProviderUrl()).thenReturn("http://provider");
            client = new CardProviderClient(restTemplate, properties);
        }

        @Test
        @DisplayName("null response throws IllegalStateException")
        void registerApplication_nullResponse_throws() {
            when(restTemplate.postForObject(anyString(), any(), eq(Map.class))).thenReturn(null);

            assertThatThrownBy(() -> client.registerApplication(UUID.randomUUID(), "Alice", 750))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("invalid response");
        }

        @Test
        @DisplayName("response missing providerRef key throws IllegalStateException")
        void registerApplication_missingKey_throws() {
            when(restTemplate.postForObject(anyString(), any(), eq(Map.class)))
                .thenReturn(Map.of("other", "value"));

            assertThatThrownBy(() -> client.registerApplication(UUID.randomUUID(), "Alice", 750))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("invalid response");
        }
    }

    @Nested
    @DisplayName("NotificationClient")
    class NotificationClientTests {

        NotificationClient client;

        @BeforeEach
        void init() {
            when(api.getNotificationUrl()).thenReturn("http://notify");
            client = new NotificationClient(restTemplate, properties);
        }

        @Test
        @DisplayName("null response throws IllegalStateException")
        void sendApprovalNotification_nullResponse_throws() {
            when(restTemplate.postForObject(anyString(), any(), eq(Map.class))).thenReturn(null);

            assertThatThrownBy(() -> client.sendApprovalNotification("a@b.com", "Alice", "REF-1"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("invalid response");
        }

        @Test
        @DisplayName("response missing notificationId key throws IllegalStateException")
        void sendApprovalNotification_missingKey_throws() {
            when(restTemplate.postForObject(anyString(), any(), eq(Map.class)))
                .thenReturn(Map.of("other", "value"));

            assertThatThrownBy(() -> client.sendApprovalNotification("a@b.com", "Alice", "REF-1"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("invalid response");
        }
    }
}
