package com.demo.outbox.pipeline.step.ingestion;

import com.demo.outbox.pipeline.PipelineStep;
import com.demo.outbox.pipeline.StepMode;
import com.demo.outbox.pipeline.context.DataIngestionContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Step 2 — Writes an internal audit note recording the Mastercard match.
 *
 * Input:  mastercardMatchId (from context — set by step 1)
 * Output: notesId (best-effort)
 *
 * FIRE_AND_FORGET: this is an internal record-keeping call. Nothing later
 * in the pipeline depends on it, so a failure is logged and the pipeline
 * proceeds — it must never dead-letter an otherwise-successful ingestion.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class InternalNotesStep implements PipelineStep<DataIngestionContext> {

    // Inject your InternalNotesClient here
    // private final InternalNotesClient internalNotesClient;

    @Override
    public String getPipelineType() { return DetokenisationStep.PIPELINE_TYPE; }

    @Override
    public int getStepIndex() { return 2; }

    @Override
    public Class<DataIngestionContext> getContextClass() { return DataIngestionContext.class; }

    @Override
    public StepMode getMode() { return StepMode.FIRE_AND_FORGET; }

    @Override
    public boolean execute(DataIngestionContext ctx) throws Exception {
        log.info("[Step 2] Writing internal note for recordId={}", ctx.getRecordId());

        // String notesId = internalNotesClient.addNote(ctx.getRecordId(),
        //     "Mastercard match: " + ctx.getMastercardMatchId());
        // TODO: replace with real InternalNotesClient call
        ctx.setNotesId("NOTE-" + ctx.getRecordId());

        log.info("[Step 2] Internal note recorded, notesId={}", ctx.getNotesId());
        return true;
    }
}
