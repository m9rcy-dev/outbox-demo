package com.demo.outbox.pipeline.step;

import com.demo.outbox.entity.CardApplication;
import com.demo.outbox.pipeline.PipelineStep;
import com.demo.outbox.pipeline.context.CardApplicationContext;
import com.demo.outbox.repository.CardApplicationRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Step 3 — Final database update. Marks the application as COMPLETED.
 *
 * This is deliberately the LAST step so that COMPLETED status is only set
 * after every external API call has succeeded. No API calls here — pure DB.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class FinalDbUpdateStep implements PipelineStep<CardApplicationContext> {

    private final CardApplicationRepository cardApplicationRepository;

    @Override
    public String getPipelineType() { return CreditBureauCheckStep.PIPELINE_TYPE; }

    @Override
    public int getStepIndex() { return 3; }

    @Override
    public Class<CardApplicationContext> getContextClass() {
        return CardApplicationContext.class;
    }

    @Override
    public boolean execute(CardApplicationContext ctx) {
        log.info("[Step 3] Finalising applicationId={}", ctx.getApplicationId());

        CardApplication app = cardApplicationRepository.findById(ctx.getApplicationId())
                .orElseThrow(() -> new IllegalStateException(
                        "CardApplication not found: " + ctx.getApplicationId()));
        app.setStatus(CardApplication.Status.COMPLETED);
        cardApplicationRepository.save(app);
        log.info("[Step 3] Application {} marked COMPLETED", app.getId());

        return true;
    }
}
