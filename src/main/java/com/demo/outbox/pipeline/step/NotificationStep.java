package com.demo.outbox.pipeline.step;

import com.demo.outbox.api.NotificationClient;
import com.demo.outbox.entity.CardApplication;
import com.demo.outbox.pipeline.PipelineStep;
import com.demo.outbox.pipeline.context.CardApplicationContext;
import com.demo.outbox.repository.CardApplicationRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Step 2 — Sends an approval notification to the applicant.
 *
 * Input:  email, applicantName, providerRef (from context)
 * Output: notificationId (set on context for step 3 to use)
 * Side effect: updates CardApplication.status → NOTIFIED
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class NotificationStep implements PipelineStep<CardApplicationContext> {

    private final NotificationClient notificationClient;
    private final CardApplicationRepository cardApplicationRepository;

    @Override
    public String getPipelineType() { return CreditBureauCheckStep.PIPELINE_TYPE; }

    @Override
    public int getStepIndex() { return 2; }

    @Override
    public Class<CardApplicationContext> getContextClass() {
        return CardApplicationContext.class;
    }

    @Override
    public boolean execute(CardApplicationContext ctx) throws Exception {
        log.info("[Step 2] Sending notification to email={}", ctx.getEmail());

        String notifId = notificationClient.sendApprovalNotification(
            ctx.getEmail(),
            ctx.getApplicantName(),
            ctx.getProviderRef()
        );
        ctx.setNotificationId(notifId);

        CardApplication app = cardApplicationRepository.findById(ctx.getApplicationId())
                .orElseThrow(() -> new IllegalStateException(
                        "CardApplication not found: " + ctx.getApplicationId()));
        app.setNotificationId(notifId);
        app.setStatus(CardApplication.Status.NOTIFIED);
        cardApplicationRepository.save(app);

        log.info("[Step 2] Notification sent, id={}", notifId);
        return true;
    }
}
