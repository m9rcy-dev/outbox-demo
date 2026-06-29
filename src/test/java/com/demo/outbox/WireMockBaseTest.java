package com.demo.outbox;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static com.github.tomakehurst.wiremock.client.WireMock.*;

/**
 * Base class that starts a single WireMock server and wires its port
 * into Spring's app.api.* properties so all clients hit the stub.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
public abstract class WireMockBaseTest {

    protected static WireMockServer wireMock = new WireMockServer(
        WireMockConfiguration.wireMockConfig().dynamicPort()
    );

    static {
        wireMock.start();
    }

    @DynamicPropertySource
    static void overrideApiUrls(DynamicPropertyRegistry registry) {
        String base = "http://localhost:" + wireMock.port();
        registry.add("app.api.credit-bureau-url", () -> base);
        registry.add("app.api.card-provider-url",  () -> base);
        registry.add("app.api.notification-url",   () -> base);
    }

    @BeforeEach
    void resetWireMock() {
        wireMock.resetAll();
    }

    // ── Stub helpers ─────────────────────────────────────────────────────────

    protected void stubCreditBureau(int score) {
        wireMock.stubFor(post(urlEqualTo("/credit/check"))
            .willReturn(okJson("{\"creditScore\": " + score + "}")));
    }

    protected void stubCreditBureauFailure() {
        wireMock.stubFor(post(urlEqualTo("/credit/check"))
            .willReturn(serverError()));
    }

    protected void stubCardProvider(String ref) {
        wireMock.stubFor(post(urlEqualTo("/cards/register"))
            .willReturn(okJson("{\"providerRef\": \"" + ref + "\"}")));
    }

    protected void stubCardProviderFailure() {
        wireMock.stubFor(post(urlEqualTo("/cards/register"))
            .willReturn(serverError()));
    }

    protected void stubNotification(String notifId) {
        wireMock.stubFor(post(urlEqualTo("/notifications/send"))
            .willReturn(okJson("{\"notificationId\": \"" + notifId + "\"}")));
    }

    protected void stubNotificationFailure() {
        wireMock.stubFor(post(urlEqualTo("/notifications/send"))
            .willReturn(serverError()));
    }

    /** Convenience: stub all three APIs for a happy-path run */
    protected void stubAllApisSuccess() {
        stubCreditBureau(750);
        stubCardProvider("PROV-REF-001");
        stubNotification("NOTIF-001");
    }
}
