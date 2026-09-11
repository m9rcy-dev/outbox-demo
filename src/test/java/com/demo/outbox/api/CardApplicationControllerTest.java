package com.demo.outbox.api;

import com.demo.outbox.WireMockBaseTest;
import com.demo.outbox.entity.CardApplication;
import com.demo.outbox.repository.CardApplicationRepository;
import com.demo.outbox.repository.OutboxEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.client.RestTestClient;

import java.util.Map;

import static org.assertj.core.api.Assertions.*;

@DisplayName("CardApplicationController integration tests")
class CardApplicationControllerTest extends WireMockBaseTest {

    @LocalServerPort int port;
    @Autowired CardApplicationRepository cardApplicationRepository;
    @Autowired OutboxEventRepository outboxEventRepository;

    private RestTestClient client;

    @BeforeEach
    void cleanDb() {
        outboxEventRepository.deleteAll();
        cardApplicationRepository.deleteAll();
        client = RestTestClient.bindToServer().baseUrl("http://localhost:" + port).build();
    }

    @Test
    @DisplayName("POST /api/v1/card-applications returns 202 ACCEPTED with entity")
    void post_returns202WithEntity() {
        Map<String, Object> body = Map.of(
            "applicantName", "Grace Lee",
            "email", "grace@example.com",
            "annualIncome", 95000
        );

        CardApplication result = client.post()
            .uri("/api/v1/card-applications")
            .contentType(MediaType.APPLICATION_JSON)
            .body(body)
            .exchange()
            .expectStatus().isAccepted()
            .expectBody(CardApplication.class)
            .returnResult()
            .getResponseBody();

        assertThat(result).isNotNull();
        assertThat(result.getId()).isNotNull();
        assertThat(result.getStatus()).isEqualTo(CardApplication.Status.SUBMITTED);
    }

    @Test
    @DisplayName("GET /api/v1/card-applications/{id} returns application by id")
    void get_returnsApplicationById() {
        Map<String, Object> body = Map.of(
            "applicantName", "Henry Ford",
            "email", "henry@example.com",
            "annualIncome", 120000
        );

        CardApplication submitted = client.post()
            .uri("/api/v1/card-applications")
            .contentType(MediaType.APPLICATION_JSON)
            .body(body)
            .exchange()
            .expectStatus().isAccepted()
            .expectBody(CardApplication.class)
            .returnResult()
            .getResponseBody();

        String id = submitted.getId().toString();

        CardApplication fetched = client.get()
            .uri("/api/v1/card-applications/" + id)
            .exchange()
            .expectStatus().isOk()
            .expectBody(CardApplication.class)
            .returnResult()
            .getResponseBody();

        assertThat(fetched.getId().toString()).isEqualTo(id);
    }

    @Test
    @DisplayName("POST with invalid email returns 400")
    void post_invalidEmail_returns400() {
        Map<String, Object> body = Map.of(
            "applicantName", "Bad User",
            "email", "not-an-email",
            "annualIncome", 50000
        );

        client.post()
            .uri("/api/v1/card-applications")
            .contentType(MediaType.APPLICATION_JSON)
            .body(body)
            .exchange()
            .expectStatus().isBadRequest();
    }

    @Test
    @DisplayName("POST with X-Request-ID header propagates it (response still 202)")
    void post_withRequestId_returns202() {
        Map<String, Object> body = Map.of(
            "applicantName", "Iris Kim",
            "email", "iris@example.com",
            "annualIncome", 75000
        );

        CardApplication result = client.post()
            .uri("/api/v1/card-applications")
            .contentType(MediaType.APPLICATION_JSON)
            .header("X-Request-ID", "client-req-001")
            .body(body)
            .exchange()
            .expectStatus().isAccepted()
            .expectBody(CardApplication.class)
            .returnResult()
            .getResponseBody();

        assertThat(result).isNotNull();
    }

    @Test
    @DisplayName("POST with negative income returns 400")
    void post_negativeIncome_returns400() {
        Map<String, Object> body = Map.of(
            "applicantName", "Negative Nancy",
            "email", "valid@example.com",
            "annualIncome", -1000
        );

        client.post()
            .uri("/api/v1/card-applications")
            .contentType(MediaType.APPLICATION_JSON)
            .body(body)
            .exchange()
            .expectStatus().isBadRequest();
    }
}
