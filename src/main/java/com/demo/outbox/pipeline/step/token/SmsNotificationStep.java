package com.demo.outbox.pipeline.step.token;

import com.demo.outbox.pipeline.PipelineStep;
import com.demo.outbox.pipeline.context.TokenUpdateContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Step 3 — Sends an SMS to the cardholder confirming the token status change.
 *
 * Input:  cardholderPhone, updatedStatus (from context)
 * Output: smsMessageId (set on context)
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class SmsNotificationStep implements PipelineStep<TokenUpdateContext> {

    // Inject your SmsClient here
    // private final SmsClient smsClient;

    @Override
    public String getPipelineType() { return MastercardTokenUpdateStep.PIPELINE_TYPE; }

    @Override
    public int getStepIndex() { return 3; }

    @Override
    public Class<TokenUpdateContext> getContextClass() { return TokenUpdateContext.class; }

    @Override
    public boolean execute(TokenUpdateContext ctx) throws Exception {
        log.info("[Step 3] Sending SMS to phone={} for cardId={}", ctx.getCardholderPhone(), ctx.getCardId());

        // String smsId = smsClient.send(ctx.getCardholderPhone(),
        //     "Your card token status has been updated to: " + ctx.getUpdatedStatus());
        // ctx.setSmsMessageId(smsId);

        // TODO: replace with real SmsClient call
        ctx.setSmsMessageId("SMS-" + ctx.getCardId());
        log.info("[Step 3] SMS sent id={}", ctx.getSmsMessageId());
        return true;
    }
}
