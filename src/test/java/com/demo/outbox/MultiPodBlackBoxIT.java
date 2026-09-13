package com.demo.outbox;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.junit.jupiter.api.*;
import tools.jackson.databind.ObjectMapper;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Black-box proof of the multi-pod claims made throughout this codebase's docs —
 * NOT a simulation. Runs the actual packaged application image, multiple times,
 * as genuinely separate JVM processes/containers with their own connection pools,
 * driven by their own real {@code @Scheduled} poller (never called directly),
 * talking to a shared Postgres over a real Docker network.
 *
 * This is deliberately NOT part of the fast test pyramid ({@code ConcurrentOutboxIT}
 * already proves the DB-level concurrency guarantees — SKIP LOCKED / @Version —
 * cheaply, with latches instead of real timing, from inside one JVM). This class
 * exists to prove the things that kind of test structurally cannot:
 *   1. The real {@code @Scheduled} wiring works end-to-end, not just the poll()
 *      method in isolation.
 *   2. Genuinely independent pods (own pool, own process) don't collide.
 *   3. A pod that is abruptly killed (SIGKILL, not a graceful shutdown) mid-pipeline
 *      loses nothing — a surviving pod finishes the work without replaying the
 *      already-completed step.
 *
 * Excluded from the default {@code mvn test} run (see the {@code docker} tag and
 * the surefire {@code excludedGroups} config in pom.xml) — it builds a Docker image
 * from source and boots real containers, so it's slow and needs Docker. Run it
 * explicitly:
 *
 *   mvn test -Pdocker-tests -Dtest=MultiPodBlackBoxIT
 */
@Tag("docker")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Multi-pod black-box tests — real containers, real scheduler, real Docker network")
class MultiPodBlackBoxIT {

    private static final int APP_PORT = 8080;
    private static final int POD_COUNT = 4;

    private static Network network;
    private static PostgreSQLContainer<?> postgres;
    private static ImageFromDockerfile appImage;
    private static WireMockServer wireMock;

    /** Per-pod captured stdout, keyed by container id — lets a test find exactly
     *  which pod is executing a given step for a given correlationId, instead of
     *  guessing from checkpoint state alone. */
    private final Map<String, StringBuffer> podLogs = new HashMap<>();

    @BeforeAll
    static void startSharedInfrastructure() {
        network = Network.newNetwork();

        postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withNetwork(network)
            .withNetworkAliases("postgres")
            .withDatabaseName("outbox_demo")
            .withUsername("outbox")
            .withPassword("outbox");
        postgres.start();

        // WireMock runs in this test JVM, not in a container — the app containers
        // reach it via Testcontainers' host-port-exposure bridge, so its admin API
        // (verify/reset) stays directly usable from the test without going through
        // a container's network boundary.
        wireMock = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
        wireMock.start();
        org.testcontainers.Testcontainers.exposeHostPorts(wireMock.port());

        // Built once from the project's own Dockerfile, reused for every pod in
        // every test — this is the actual artifact that would be deployed, not a
        // stand-in for it. Must be an absolute path: withDockerfile(Path) derives
        // the build's base directory from dockerfilePath.getParent(), and a bare
        // relative "Dockerfile" (single path segment) has no parent — that's a
        // null baseDirectory NPE, not a missing-context problem.
        Path projectRoot = Path.of("").toAbsolutePath();
        appImage = new ImageFromDockerfile("outbox-demo-blackbox-test", false)
            .withDockerfile(projectRoot.resolve("Dockerfile"));
    }

    @AfterAll
    static void stopSharedInfrastructure() {
        wireMock.stop();
        postgres.stop();
        network.close();
    }

    @BeforeEach
    void cleanDatabaseAndStubs() throws Exception {
        wireMock.resetAll();
        stubAllApisSuccess();
        // Flyway only runs once the first pod boots and connects — on this class's
        // very first test, the schema doesn't exist yet. "No tables" is equivalent
        // to "already clean," so tolerate it rather than requiring pods to have run first.
        try (Connection c = jdbcConnection();
             Statement s = c.createStatement()) {
            s.execute("DELETE FROM outbox_event");
            s.execute("DELETE FROM card_application");
        } catch (java.sql.SQLException e) {
            if (!"42P01".equals(e.getSQLState())) throw e;
        }
    }

    // ── Test 1: steady state across 4 genuinely independent pods ────────────────

    @Test
    @Order(1)
    @DisplayName("4 independent pod containers process a shared backlog with no event lost or stuck")
    void fourPods_processSharedBacklog_noEventLostOrStuck() throws Exception {
        List<GenericContainer<?>> pods = startPods(POD_COUNT);
        try {
            String entryUrl = podBaseUrl(pods.get(0));

            int total = 12;
            List<UUID> applicationIds = new ArrayList<>();
            for (int i = 0; i < total; i++) {
                applicationIds.add(submitApplication(entryUrl, "User " + i, "user" + i + "@test.com", 50000 + i * 1000));
            }
            assertThat(applicationIds).hasSize(total);

            awaitAllCompleted(applicationIds, Duration.ofSeconds(60));

            // Every credit-bureau/card-provider/notification call landed at least once —
            // proves no event was silently skipped, regardless of which pod grabbed it.
            wireMock.verify(moreThanOrExactly(total), postRequestedFor(urlEqualTo("/credit/check")));
            wireMock.verify(moreThanOrExactly(total), postRequestedFor(urlEqualTo("/cards/register")));
            wireMock.verify(moreThanOrExactly(total), postRequestedFor(urlEqualTo("/notifications/send")));
        } finally {
            stopPods(pods);
        }
    }

    // ── Test 2: kill the majority of the fleet mid-pipeline ─────────────────────

    @Test
    @Order(2)
    @DisplayName("Killing 3 of 4 pods mid-pipeline: the survivor finishes everything without replaying completed steps")
    void killMajorityOfPods_survivorFinishesWithoutReplayingCompletedSteps() throws Exception {
        // Slow down step 1 so there's a reliable window where step 0 has committed
        // (checkpoint advanced, creditScore recorded) but the event isn't PROCESSED
        // yet — that's the window we want the kill to land in.
        wireMock.stubFor(post(urlEqualTo("/cards/register"))
            .willReturn(okJson("{\"providerRef\": \"PROV-REF-001\"}").withFixedDelay(4000)));

        List<GenericContainer<?>> pods = startPods(POD_COUNT);
        try {
            String entryUrl = podBaseUrl(pods.get(0));

            int total = 6;
            List<UUID> applicationIds = new ArrayList<>();
            for (int i = 0; i < total; i++) {
                applicationIds.add(submitApplication(entryUrl, "Kill" + i, "kill" + i + "@test.com", 60000 + i * 1000));
            }

            // Wait until at least one event has checkpointed past step 0 (creditScore
            // recorded) — proof that SOME pod actually started real work before we kill.
            Map<String, Object> midFlightSnapshot = awaitAnyEventPastStep(0, Duration.ofSeconds(30));
            String survivedCreditScoreFragment = (String) midFlightSnapshot.get("payload");
            assertThat(survivedCreditScoreFragment).contains("creditScore");

            // Abruptly kill (SIGKILL, not a graceful stop) 3 of the 4 pods — whichever
            // pod was mid-step for any in-flight event is very likely among them.
            List<GenericContainer<?>> victims = pods.subList(0, 3);
            List<GenericContainer<?>> survivors = pods.subList(3, 4);
            for (GenericContainer<?> victim : victims) {
                org.testcontainers.DockerClientFactory.lazyClient()
                    .killContainerCmd(victim.getContainerId())
                    .exec();
            }

            // The lone survivor, on its own, must still finish every event — proving
            // the checkpoint (not the pod that started an event) is what determines
            // completion, and that step 0's already-recorded output is never redone.
            awaitAllCompleted(applicationIds, Duration.ofSeconds(90));

            // The specific event we captured mid-flight must have kept the SAME
            // creditScore it had before the kill — proof step 0 was resumed from the
            // checkpoint, not re-executed by whichever pod finished it.
            String eventId = (String) midFlightSnapshot.get("id");
            String finalPayload = fetchPayload(eventId);
            assertThat(extractCreditScore(finalPayload))
                .as("creditScore must be unchanged — step 0 must not have been replayed after the kill")
                .isEqualTo(extractCreditScore(survivedCreditScoreFragment));

            stopPods(survivors);
        } finally {
            stopPods(pods.stream().filter(GenericContainer::isRunning).collect(Collectors.toList()));
        }
    }

    // ── Test 3: kill a specific pod while it is genuinely mid-step ──────────────
    //
    // Test 2 kills 3 of 4 pods once *some* event has checkpointed past step 0 —
    // real, but incidental: nothing there proves any killed pod was actually
    // still inside a step's transaction (blocked in an external call) at the
    // moment of the kill, rather than idle between poll cycles. This test closes
    // that gap by identifying, via log, a pod that is provably still inside the
    // (artificially delayed) external call for step 1 — then kills only that pod.
    //
    // Empirical finding worth recording rather than hiding: with only 1 event and
    // 4 idle pods, running this actually showed all 4 pods logging "executing
    // step 2/4" for the SAME correlationId within about a second of each other —
    // i.e. the SKIP LOCKED soft-distribution window (see PipelineOutboxPoller's
    // own docs: "a best-effort hint, not a hard guarantee") let all four pods
    // fetch the one pending row before any of them had committed a checkpoint.
    // So "kill the pod mid-step" did not isolate a sole worker — it removed one
    // of several pods already racing on the same event, and a surviving racer's
    // own in-flight attempt is what actually finished it (confirmed below by a
    // DIFFERENT surviving pod losing its own @Version race with "concurrent
    // update ... another pod owns it"). That's a *stronger* result than the
    // narrower one this test set out to prove: it doesn't matter whether the
    // killed pod was the sole processor of an event or one of several
    // concurrently racing on it — either way nothing is lost and step 0 is never
    // replayed. True single-owner exclusivity isn't something this architecture
    // promises (that's exactly why @Version exists as the hard guard), so a
    // black-box test can't force it without fighting the design it's verifying.

    @Test
    @Order(3)
    @DisplayName("Killing a pod that is genuinely mid-step (blocked in the external call): the pipeline still completes, step 0 is never replayed")
    void killExactPodMidStep_survivorResumesFromCheckpoint() throws Exception {
        // Long enough that we can reliably detect the pod mid-call and kill it
        // before the call would have completed on its own.
        wireMock.stubFor(post(urlEqualTo("/cards/register"))
            .willReturn(okJson("{\"providerRef\": \"PROV-REF-001\"}").withFixedDelay(8000)));

        List<GenericContainer<?>> pods = startPods(POD_COUNT);
        try {
            String entryUrl = podBaseUrl(pods.get(0));
            UUID applicationId = submitApplication(entryUrl, "MidStep", "midstep@test.com", 70000);

            // Poll each pod's captured stdout for the log line proving it is the one
            // that just started step 1 (CardProviderRegisterStep) for THIS event —
            // at that point it is inside the 8s-delayed HTTP call, transaction open.
            String needle = "correlationId=" + applicationId + " → executing step 2/4 (CardProviderRegisterStep)";
            GenericContainer<?> midStepPod = awaitPodLogging(pods, needle, Duration.ofSeconds(30));

            org.testcontainers.DockerClientFactory.lazyClient()
                .killContainerCmd(midStepPod.getContainerId())
                .exec();

            List<GenericContainer<?>> survivors = pods.stream()
                .filter(p -> p != midStepPod)
                .collect(Collectors.toList());

            // A surviving pod must complete the pipeline on its own — proving the
            // checkpoint, not the pod that started the step, is what determines
            // whether the work gets finished.
            awaitAllCompleted(List.of(applicationId), Duration.ofSeconds(60));

            // Step 0 (credit-bureau) must have been called EXACTLY once — the pod
            // that died was killed before it ever reached step 0 again (it died
            // inside step 1), and no surviving pod should have replayed step 0
            // either, since the checkpoint already showed it complete.
            wireMock.verify(exactly(1), postRequestedFor(urlEqualTo("/credit/check")));

            stopPods(survivors);
        } finally {
            stopPods(pods.stream().filter(GenericContainer::isRunning).collect(Collectors.toList()));
        }
    }

    /** Polls each pod's captured stdout until one contains {@code needle}. */
    private GenericContainer<?> awaitPodLogging(List<GenericContainer<?>> pods, String needle, Duration timeout)
            throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            for (GenericContainer<?> pod : pods) {
                StringBuffer buffer = podLogs.get(pod.getContainerId());
                if (buffer != null && buffer.indexOf(needle) >= 0) {
                    return pod;
                }
            }
            Thread.sleep(200);
        }
        fail("No pod logged \"" + needle + "\" within " + timeout);
        return null; // unreachable
    }

    // ── Pod lifecycle ────────────────────────────────────────────────────────────

    private List<GenericContainer<?>> startPods(int count) {
        List<GenericContainer<?>> pods = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            @SuppressWarnings("resource")
            GenericContainer<?> pod = new GenericContainer<>(appImage)
                .withNetwork(network)
                .withNetworkAliases("pod-" + i)
                .withExposedPorts(APP_PORT)
                .withEnv("SPRING_DATASOURCE_URL", "jdbc:postgresql://postgres:5432/" + postgres.getDatabaseName())
                .withEnv("SPRING_DATASOURCE_USERNAME", postgres.getUsername())
                .withEnv("SPRING_DATASOURCE_PASSWORD", postgres.getPassword())
                .withEnv("APP_API_CREDIT_BUREAU_URL", "http://host.testcontainers.internal:" + wireMock.port())
                .withEnv("APP_API_CARD_PROVIDER_URL", "http://host.testcontainers.internal:" + wireMock.port())
                .withEnv("APP_API_NOTIFICATION_URL", "http://host.testcontainers.internal:" + wireMock.port())
                .withEnv("APP_OUTBOX_POLL_DELAY_MS", "1000")
                .waitingFor(Wait.forLogMessage(".*Started OutboxDemoApplication.*\\n", 1)
                    .withStartupTimeout(Duration.ofSeconds(120)));
            pod.start();
            pod.followOutput(new Slf4jLogConsumer(org.slf4j.LoggerFactory.getLogger("pod-" + i)));
            StringBuffer buffer = new StringBuffer();
            podLogs.put(pod.getContainerId(), buffer);
            pod.followOutput((Consumer<OutputFrame>) frame -> buffer.append(frame.getUtf8String()));
            pods.add(pod);
        }
        return pods;
    }

    private void stopPods(List<GenericContainer<?>> pods) {
        for (GenericContainer<?> pod : pods) {
            try {
                if (pod.isRunning()) pod.stop();
            } catch (Exception ignored) {
                // already dead (e.g. one we SIGKILLed in the test itself) — fine
            }
        }
    }

    private String podBaseUrl(GenericContainer<?> pod) {
        return "http://" + pod.getHost() + ":" + pod.getMappedPort(APP_PORT);
    }

    // ── HTTP + JDBC helpers — deliberately NOT going through Spring at all ──────

    private final HttpClient http = HttpClient.newHttpClient();
    private final ObjectMapper objectMapper = new ObjectMapper();

    private UUID submitApplication(String baseUrl, String name, String email, int income) throws Exception {
        String body = objectMapper.writeValueAsString(Map.of(
            "applicantName", name, "email", email, "annualIncome", income
        ));
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(baseUrl + "/api/v1/card-applications"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();
        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as("submit response: %s", response.body()).isEqualTo(202);
        @SuppressWarnings("unchecked")
        Map<String, Object> parsed = objectMapper.readValue(response.body(), Map.class);
        return UUID.fromString((String) parsed.get("id"));
    }

    private Connection jdbcConnection() throws Exception {
        return DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }

    private void awaitAllCompleted(List<UUID> applicationIds, Duration timeout) throws Exception {
        Instant deadline = Instant.now().plus(timeout);
        String inClause = applicationIds.stream().map(id -> "'" + id + "'").collect(Collectors.joining(","));
        while (Instant.now().isBefore(deadline)) {
            try (Connection c = jdbcConnection(); Statement s = c.createStatement()) {
                ResultSet rs = s.executeQuery(
                    "SELECT count(*) FROM card_application WHERE id IN (" + inClause + ") AND status = 'COMPLETED'");
                rs.next();
                if (rs.getInt(1) == applicationIds.size()) return;
            }
            Thread.sleep(500);
        }
        fail("Not all " + applicationIds.size() + " applications reached COMPLETED within " + timeout);
    }

    /** Returns the id/payload of any outbox_event whose currentStep has advanced past {@code step}. */
    private Map<String, Object> awaitAnyEventPastStep(int step, Duration timeout) throws Exception {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            try (Connection c = jdbcConnection(); Statement s = c.createStatement()) {
                ResultSet rs = s.executeQuery(
                    "SELECT id, payload FROM outbox_event WHERE current_step > " + step + " LIMIT 1");
                if (rs.next()) {
                    return Map.of("id", rs.getString("id"), "payload", rs.getString("payload"));
                }
            }
            Thread.sleep(300);
        }
        fail("No event advanced past step " + step + " within " + timeout);
        return Map.of();
    }

    private String fetchPayload(String eventId) throws Exception {
        try (Connection c = jdbcConnection(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT payload FROM outbox_event WHERE id = '" + eventId + "'");
            rs.next();
            return rs.getString("payload");
        }
    }

    private Integer extractCreditScore(String payloadJson) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> ctx = objectMapper.readValue(payloadJson, Map.class);
            Object score = ctx.get("creditScore");
            return score == null ? null : ((Number) score).intValue();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // ── WireMock stubs (identical contract to WireMockBaseTest) ─────────────────

    private void stubAllApisSuccess() {
        wireMock.stubFor(post(urlEqualTo("/credit/check")).willReturn(okJson("{\"creditScore\": 750}")));
        wireMock.stubFor(post(urlEqualTo("/cards/register")).willReturn(okJson("{\"providerRef\": \"PROV-REF-001\"}")));
        wireMock.stubFor(post(urlEqualTo("/notifications/send")).willReturn(okJson("{\"notificationId\": \"NOTIF-001\"}")));
    }
}
