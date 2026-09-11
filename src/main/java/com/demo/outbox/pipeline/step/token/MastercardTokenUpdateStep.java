package com.demo.outbox.pipeline.step.token;

import com.demo.outbox.pipeline.PipelineStep;
import com.demo.outbox.pipeline.StepMode;
import com.demo.outbox.pipeline.context.TokenUpdateContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Step 0 — Updates the digital token status at Mastercard (source of truth).
 *
 * Input:  tokenId, desiredStatus (from context)
 * Output: mastercardRef, updatedStatus (set on context for downstream steps)
 *
 * The Mastercard client MUST pass a deterministic idempotency key so that
 * a retry (after a checkpoint failure) returns the same response without
 * re-executing the update on Mastercard's side.
 *
 * BLOCKING_SELF_RETRIED: the Mastercard integration client already retries
 * transient failures internally, so by the time an exception reaches this
 * step its own attempts are exhausted — the pipeline only needs one more
 * checkpoint-level retry on top, not the pipeline-wide default.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class MastercardTokenUpdateStep implements PipelineStep<TokenUpdateContext> {

    public static final String PIPELINE_TYPE = "TOKEN_UPDATE_PIPELINE";

    // Inject your MastercardClient here
    // private final MastercardClient mastercardClient;

    @Override
    public String getPipelineType() { return PIPELINE_TYPE; }

    @Override
    public int getStepIndex() { return 0; }

    @Override
    public Class<TokenUpdateContext> getContextClass() { return TokenUpdateContext.class; }

    @Override
    public StepMode getMode() { return StepMode.BLOCKING_SELF_RETRIED; }

    @Override
    public int getMaxRetries() { return 1; }

    @Override
    public boolean execute(TokenUpdateContext ctx) throws Exception {
        log.info("[Step 0] Updating Mastercard token={} to status={}", ctx.getTokenId(), ctx.getDesiredStatus());

        // Deterministic across every retry of this same outbox event — Mastercard
        // dedupes on this key instead of applying the status change twice.
        String idempotencyKey = ctx.getCardId() + ":" + ctx.getTokenId() + ":" + ctx.getDesiredStatus();

        // MastercardTokenResponse response = mastercardClient.updateTokenStatus(
        //     ctx.getTokenId(), ctx.getDesiredStatus(), idempotencyKey);

        // TODO: replace with real MastercardClient call
        String mastercardRef = "MC-REF-" + idempotencyKey;
        ctx.setMastercardRef(mastercardRef);
        ctx.setUpdatedStatus(ctx.getDesiredStatus());

        log.info("[Step 0] Mastercard confirmed token={} status={} ref={}", ctx.getTokenId(), ctx.getUpdatedStatus(), mastercardRef);
        return true;
    }
}
