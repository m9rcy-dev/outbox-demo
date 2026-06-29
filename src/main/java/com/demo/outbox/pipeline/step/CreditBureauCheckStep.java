package com.demo.outbox.pipeline.step;

import com.demo.outbox.api.CreditBureauClient;
import com.demo.outbox.entity.CardApplication;
import com.demo.outbox.pipeline.PipelineStep;
import com.demo.outbox.pipeline.context.CardApplicationContext;
import com.demo.outbox.repository.CardApplicationRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Step 0 — Calls the Credit Bureau API to get a credit score.
 *
 * Input:  applicantName, annualIncome (from context)
 * Output: creditScore   (set on context for step 1 to use)
 * Side effect: updates CardApplication.status → CREDIT_CHECKED
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class CreditBureauCheckStep implements PipelineStep<CardApplicationContext> {

    public static final String PIPELINE_TYPE = "CARD_APPLICATION_PIPELINE";

    private final CreditBureauClient creditBureauClient;
    private final CardApplicationRepository cardApplicationRepository;

    @Override
    public String getPipelineType() { return PIPELINE_TYPE; }

    @Override
    public int getStepIndex() { return 0; }

    @Override
    public Class<CardApplicationContext> getContextClass() {
        return CardApplicationContext.class;
    }

    @Override
    public boolean execute(CardApplicationContext ctx) throws Exception {
        log.info("[Step 0] Checking credit for applicationId={}", ctx.getApplicationId());

        int score = creditBureauClient.checkCredit(ctx.getApplicantName(), ctx.getAnnualIncome());
        ctx.setCreditScore(score);

        CardApplication app = cardApplicationRepository.findById(ctx.getApplicationId())
                .orElseThrow(() -> new IllegalStateException(
                        "CardApplication not found: " + ctx.getApplicationId()));
        app.setCreditScore(score);
        app.setStatus(CardApplication.Status.CREDIT_CHECKED);
        cardApplicationRepository.save(app);

        log.info("[Step 0] Credit check complete, score={}", score);
        return true;
    }
}
