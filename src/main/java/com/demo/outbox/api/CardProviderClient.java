package com.demo.outbox.api;

import com.demo.outbox.config.AppConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Map;
import java.util.UUID;

/**
 * Client for the Card Provider API.
 * Registers an approved application with the card network and returns a provider reference.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class CardProviderClient {

    private final RestTemplate restTemplate;
    private final AppConfig.AppProperties properties;

    public String registerApplication(UUID applicationId, String applicantName, int creditScore) {
        String url = properties.getApi().getCardProviderUrl() + "/cards/register";
        log.debug("Registering application={} with card provider", applicationId);

        Map<String, Object> request = Map.of(
            "applicationId", applicationId.toString(),
            "applicantName", applicantName,
            "creditScore", creditScore
        );

        @SuppressWarnings("unchecked")
        Map<String, Object> response = restTemplate.postForObject(url, request, Map.class);

        if (response == null || !response.containsKey("providerRef")) {
            throw new IllegalStateException("Card provider returned invalid response");
        }

        String ref = (String) response.get("providerRef");
        log.debug("Card provider returned providerRef={}", ref);
        return ref;
    }
}
