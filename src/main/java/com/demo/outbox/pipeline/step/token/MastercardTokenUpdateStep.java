package com.demo.outbox.pipeline.step.token;

import com.demo.outbox.pipeline.PipelineStep;
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
    public boolean execute(TokenUpdateContext ctx) throws Exception {
        log.info("[Step 0] Updating Mastercard token={} to status={}", ctx.getTokenId(), ctx.getDesiredStatus());

        // Idempotency key: stable across retries for this specific saga step
        // String idempotencyKey = outboxEventId + ":step0";  // pass via ctx or inject
        // MastercardTokenResponse response = mastercardClient.updateTokenStatus(
        //     ctx.getTokenId(), ctx.getDesiredStatus(), idempotencyKey);

        // TODO: replace with real MastercardClient call
        String mastercardRef = "MC-REF-" + ctx.getTokenId();
        ctx.setMastercardRef(mastercardRef);
        ctx.setUpdatedStatus(ctx.getDesiredStatus());

        log.info("[Step 0] Mastercard confirmed token={} status={} ref={}", ctx.getTokenId(), ctx.getUpdatedStatus(), mastercardRef);
        return true;
    }
}
