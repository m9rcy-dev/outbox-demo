package com.demo.outbox.api;

import com.demo.outbox.config.AppConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.util.Map;

/**
 * Client for the Credit Bureau API.
 * Returns a credit score for an applicant based on their income.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class CreditBureauClient {

    private final RestTemplate restTemplate;
    private final AppConfig.AppProperties properties;

    public int checkCredit(String applicantName, BigDecimal annualIncome) {
        String url = properties.getApi().getCreditBureauUrl() + "/credit/check";
        log.debug("Calling credit bureau for applicant={}", applicantName);

        Map<String, Object> request = Map.of(
            "applicantName", applicantName,
            "annualIncome", annualIncome
        );

        @SuppressWarnings("unchecked")
        Map<String, Object> response = restTemplate.postForObject(url, request, Map.class);

        if (response == null || !response.containsKey("creditScore")) {
            throw new IllegalStateException("Credit bureau returned invalid response");
        }

        int score = ((Number) response.get("creditScore")).intValue();
        log.debug("Credit bureau returned score={} for applicant={}", score, applicantName);
        return score;
    }
}
