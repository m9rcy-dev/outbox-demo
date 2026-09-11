package com.demo.outbox.pipeline.step.token;

import com.demo.outbox.pipeline.PipelineStep;
import com.demo.outbox.pipeline.context.TokenUpdateContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Step 1 — Replicates the confirmed Mastercard status to the local card record.
 *
 * Input:  cardId, updatedStatus, mastercardRef (from context — set by Step 0)
 * Output: none (pure DB write, no new context values)
 *
 * This step and its checkpoint commit in the same REQUIRES_NEW transaction
 * (via OutboxStepExecutor), so the local status and the saga progress are
 * always in sync — either both persist or both roll back.
 *
 * BLOCKING_REQUIRED (the default — no override needed): this is a plain
 * internal DB write with no external dependency, so it should keep retrying
 * against the pipeline-wide max-retries until it succeeds rather than being
 * given a short leash. Letting this step dead-letter early is worse than a
 * few extra retries — it would leave Mastercard's status permanently out of
 * sync with the local record with no automatic recovery.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class LocalCardStatusSyncStep implements PipelineStep<TokenUpdateContext> {

    // Inject your CardTokenRepository here
    // private final CardTokenRepository cardTokenRepository;

    @Override
    public String getPipelineType() { return MastercardTokenUpdateStep.PIPELINE_TYPE; }

    @Override
    public int getStepIndex() { return 1; }

    @Override
    public Class<TokenUpdateContext> getContextClass() { return TokenUpdateContext.class; }

    @Override
    public boolean execute(TokenUpdateContext ctx) {
        log.info("[Step 1] Syncing local status for cardId={} to status={} ref={}",
            ctx.getCardId(), ctx.getUpdatedStatus(), ctx.getMastercardRef());

        // CardToken token = cardTokenRepository.findById(ctx.getCardId()).orElseThrow();
        // token.setStatus(ctx.getUpdatedStatus());
        // token.setMastercardRef(ctx.getMastercardRef());
        // cardTokenRepository.save(token);

        // TODO: replace with real CardTokenRepository call
        log.info("[Step 1] Local status synced for cardId={}", ctx.getCardId());
        return true;
    }
}
