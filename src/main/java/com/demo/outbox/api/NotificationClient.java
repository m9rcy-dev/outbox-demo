package com.demo.outbox.api;

import com.demo.outbox.config.AppConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

/**
 * Client for the Notification API.
 * Sends an email/SMS notification to the applicant.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class NotificationClient {

    private final RestTemplate restTemplate;
    private final AppConfig.AppProperties properties;

    public String sendApprovalNotification(String email, String applicantName, String providerRef) {
        String url = properties.getApi().getNotificationUrl() + "/notifications/send";
        log.debug("Sending notification to email={}", email);

        Map<String, Object> request = Map.of(
            "email", email,
            "subject", "Your card application has been approved",
            "body", "Dear " + applicantName + ", your card ref " + providerRef + " is approved."
        );

        @SuppressWarnings("unchecked")
        Map<String, Object> response = restTemplate.postForObject(url, request, Map.class);

        if (response == null || !response.containsKey("notificationId")) {
            throw new IllegalStateException("Notification service returned invalid response");
        }

        String notifId = (String) response.get("notificationId");
        log.debug("Notification sent, id={}", notifId);
        return notifId;
    }
}
