package com.demo.outbox;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static com.github.tomakehurst.wiremock.client.WireMock.*;

/**
 * Base class for integration tests that require a real PostgreSQL database.
 *
 * A single container is shared for all subclasses via the static field.
 * Spring Boot's @ServiceConnection wires the container's JDBC URL/credentials
 * into spring.datasource.* automatically, no @DynamicPropertySource needed for that.
 *
 * Does NOT extend WireMockBaseTest — that class hardcodes @ActiveProfiles("test")
 * which would activate the H2 datasource config. WireMock infrastructure is
 * reproduced here under @ActiveProfiles("postgres-test").
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("postgres-test")
@Testcontainers
public abstract class PostgresBaseTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

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

    protected void stubAllApisSuccess() {
        stubCreditBureau(750);
        stubCardProvider("PROV-REF-001");
        stubNotification("NOTIF-001");
    }
}
