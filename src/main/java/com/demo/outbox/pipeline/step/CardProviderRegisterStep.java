package com.demo.outbox.pipeline.step;

import com.demo.outbox.api.CardProviderClient;
import com.demo.outbox.entity.CardApplication;
import com.demo.outbox.pipeline.PipelineStep;
import com.demo.outbox.pipeline.context.CardApplicationContext;
import com.demo.outbox.repository.CardApplicationRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Step 1 — Registers the application with the card provider network.
 *
 * Input:  applicationId, applicantName, creditScore (from context)
 * Output: providerRef  (set on context for step 2 to use)
 * Side effect: updates CardApplication.status → PROVIDER_REGISTERED
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class CardProviderRegisterStep implements PipelineStep<CardApplicationContext> {

    private final CardProviderClient cardProviderClient;
    private final CardApplicationRepository cardApplicationRepository;

    @Override
    public String getPipelineType() { return CreditBureauCheckStep.PIPELINE_TYPE; }

    @Override
    public int getStepIndex() { return 1; }

    @Override
    public Class<CardApplicationContext> getContextClass() {
        return CardApplicationContext.class;
    }

    @Override
    public boolean execute(CardApplicationContext ctx) throws Exception {
        log.info("[Step 1] Registering applicationId={} with card provider", ctx.getApplicationId());

        String ref = cardProviderClient.registerApplication(
            ctx.getApplicationId(),
            ctx.getApplicantName(),
            ctx.getCreditScore()
        );
        ctx.setProviderRef(ref);

        CardApplication app = cardApplicationRepository.findById(ctx.getApplicationId())
                .orElseThrow(() -> new IllegalStateException(
                        "CardApplication not found: " + ctx.getApplicationId()));
        app.setProviderRef(ref);
        app.setStatus(CardApplication.Status.PROVIDER_REGISTERED);
        cardApplicationRepository.save(app);

        log.info("[Step 1] Provider registration complete, ref={}", ref);
        return true;
    }
}
