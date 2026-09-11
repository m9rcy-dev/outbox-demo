package com.demo.outbox.pipeline.step.ingestion;

import com.demo.outbox.entity.IngestionRecord;
import com.demo.outbox.pipeline.PipelineStep;
import com.demo.outbox.pipeline.StepMode;
import com.demo.outbox.pipeline.context.DataIngestionContext;
import com.demo.outbox.repository.IngestionRecordRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Step 1 — Looks up the card in the Mastercard network using the
 * detokenised PAN.
 *
 * Input:  panLast4 (from context — set by step 0)
 * Output: mastercardMatchId (set on context for downstream steps)
 *
 * BLOCKING_SELF_RETRIED: the Mastercard search client already has its own
 * retry mechanism, so an exception here means it has already exhausted its
 * own attempts. The pipeline only needs a thin retry on top (maxRetries=1)
 * rather than the pipeline-wide default — this must still succeed before
 * proceeding, since later steps report against the match.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class MastercardSearchStep implements PipelineStep<DataIngestionContext> {

    // Inject your MastercardSearchClient here (owns its own retry/backoff)
    // private final MastercardSearchClient mastercardSearchClient;

    private final IngestionRecordRepository ingestionRecordRepository;

    @Override
    public String getPipelineType() { return DetokenisationStep.PIPELINE_TYPE; }

    @Override
    public int getStepIndex() { return 1; }

    @Override
    public Class<DataIngestionContext> getContextClass() { return DataIngestionContext.class; }

    @Override
    public StepMode getMode() { return StepMode.BLOCKING_SELF_RETRIED; }

    @Override
    public int getMaxRetries() { return 1; }

    @Override
    public boolean execute(DataIngestionContext ctx) throws Exception {
        log.info("[Step 1] Searching Mastercard for recordId={}", ctx.getRecordId());

        // String matchId = mastercardSearchClient.search(ctx.getPanLast4());
        // TODO: replace with real MastercardSearchClient call
        String matchId = "MC-MATCH-" + ctx.getPanLast4();
        ctx.setMastercardMatchId(matchId);

        IngestionRecord record = ingestionRecordRepository.findById(ctx.getRecordId())
            .orElseThrow(() -> new IllegalStateException("IngestionRecord not found: " + ctx.getRecordId()));
        record.setMastercardMatchId(matchId);
        record.setStatus(IngestionRecord.Status.MASTERCARD_MATCHED);
        ingestionRecordRepository.save(record);

        log.info("[Step 1] Mastercard match found, matchId={}", matchId);
        return true;
    }
}
