package com.demo.outbox.api;

import com.demo.outbox.WireMockBaseTest;
import com.demo.outbox.entity.CardApplication;
import com.demo.outbox.repository.CardApplicationRepository;
import com.demo.outbox.repository.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;

import java.util.Map;

import static org.assertj.core.api.Assertions.*;

@DisplayName("CardApplicationController integration tests")
class CardApplicationControllerTest extends WireMockBaseTest {

    @Autowired TestRestTemplate restTemplate;
    @Autowired CardApplicationRepository cardApplicationRepository;
    @Autowired OutboxEventRepository outboxEventRepository;
    @Autowired ObjectMapper objectMapper;

    @BeforeEach
    void cleanDb() {
        outboxEventRepository.deleteAll();
        cardApplicationRepository.deleteAll();
    }

    @Test
    @DisplayName("POST /api/v1/card-applications returns 202 ACCEPTED with entity")
    void post_returns202WithEntity() throws Exception {
        Map<String, Object> body = Map.of(
            "applicantName", "Grace Lee",
            "email", "grace@example.com",
            "annualIncome", 95000
        );

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        ResponseEntity<CardApplication> response = restTemplate.exchange(
            "/api/v1/card-applications",
            HttpMethod.POST,
            new HttpEntity<>(objectMapper.writeValueAsString(body), headers),
            CardApplication.class
        );

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getId()).isNotNull();
        assertThat(response.getBody().getStatus()).isEqualTo(CardApplication.Status.SUBMITTED);
    }

    @Test
    @DisplayName("GET /api/v1/card-applications/{id} returns application by id")
    void get_returnsApplicationById() throws Exception {
        // Submit
        Map<String, Object> body = Map.of(
            "applicantName", "Henry Ford",
            "email", "henry@example.com",
            "annualIncome", 120000
        );

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        ResponseEntity<CardApplication> postResponse = restTemplate.exchange(
            "/api/v1/card-applications",
            HttpMethod.POST,
            new HttpEntity<>(objectMapper.writeValueAsString(body), headers),
            CardApplication.class
        );

        String id = postResponse.getBody().getId().toString();

        // Fetch
        ResponseEntity<CardApplication> getResponse = restTemplate.getForEntity(
            "/api/v1/card-applications/" + id,
            CardApplication.class
        );

        assertThat(getResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(getResponse.getBody().getId().toString()).isEqualTo(id);
    }

    @Test
    @DisplayName("POST with invalid email returns 400")
    void post_invalidEmail_returns400() throws Exception {
        Map<String, Object> body = Map.of(
            "applicantName", "Bad User",
            "email", "not-an-email",
            "annualIncome", 50000
        );

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        ResponseEntity<String> response = restTemplate.exchange(
            "/api/v1/card-applications",
            HttpMethod.POST,
            new HttpEntity<>(objectMapper.writeValueAsString(body), headers),
            String.class
        );

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    @DisplayName("POST with X-Request-ID header propagates it (response still 202)")
    void post_withRequestId_returns202() throws Exception {
        Map<String, Object> body = Map.of(
            "applicantName", "Iris Kim",
            "email", "iris@example.com",
            "annualIncome", 75000
        );

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("X-Request-ID", "client-req-001");

        ResponseEntity<CardApplication> response = restTemplate.exchange(
            "/api/v1/card-applications",
            HttpMethod.POST,
            new HttpEntity<>(objectMapper.writeValueAsString(body), headers),
            CardApplication.class
        );

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(response.getBody()).isNotNull();
    }

    @Test
    @DisplayName("POST with negative income returns 400")
    void post_negativeIncome_returns400() throws Exception {
        Map<String, Object> body = Map.of(
            "applicantName", "Negative Nancy",
            "email", "valid@example.com",
            "annualIncome", -1000
        );

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        ResponseEntity<String> response = restTemplate.exchange(
            "/api/v1/card-applications",
            HttpMethod.POST,
            new HttpEntity<>(objectMapper.writeValueAsString(body), headers),
            String.class
        );

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }
}
