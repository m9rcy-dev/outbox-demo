package com.demo.outbox.pipeline.step.ingestion;

import com.demo.outbox.pipeline.PipelineStep;
import com.demo.outbox.pipeline.StepMode;
import com.demo.outbox.pipeline.context.DataIngestionContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Step 3 — Sends a courtesy SMS to the cardholder confirming ingestion.
 *
 * Input:  cardholderPhone (from context)
 * Output: smsMessageId (best-effort)
 *
 * FIRE_AND_FORGET: an SMS failing (bad number, carrier outage) must never
 * hold up the MQ publish that downstream systems are waiting on.
 *
 * appliesTo: not every ingestion record has a phone number on file — this
 * step only makes sense when one was supplied. No phone means "skip", not
 * "fail"; nothing here is wrong, there's just nothing to send.
 */
@Component("ingestionSmsNotificationStep")
@RequiredArgsConstructor
@Slf4j
public class SmsNotificationStep implements PipelineStep<DataIngestionContext> {

    // Inject your SmsClient here
    // private final SmsClient smsClient;

    @Override
    public String getPipelineType() { return DetokenisationStep.PIPELINE_TYPE; }

    @Override
    public int getStepIndex() { return 3; }

    @Override
    public Class<DataIngestionContext> getContextClass() { return DataIngestionContext.class; }

    @Override
    public StepMode getMode() { return StepMode.FIRE_AND_FORGET; }

    @Override
    public boolean appliesTo(DataIngestionContext ctx) {
        return ctx.getCardholderPhone() != null && !ctx.getCardholderPhone().isBlank();
    }

    @Override
    public boolean execute(DataIngestionContext ctx) throws Exception {
        log.info("[Step 3] Sending confirmation SMS for recordId={}", ctx.getRecordId());

        // String smsId = smsClient.send(ctx.getCardholderPhone(), "Your card has been processed.");
        // TODO: replace with real SmsClient call
        ctx.setSmsMessageId("SMS-" + ctx.getRecordId());

        log.info("[Step 3] SMS sent, id={}", ctx.getSmsMessageId());
        return true;
    }
}
