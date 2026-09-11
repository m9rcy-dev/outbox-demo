package com.demo.outbox.pipeline.step.ingestion;

import com.demo.outbox.entity.IngestionRecord;
import com.demo.outbox.pipeline.PipelineStep;
import com.demo.outbox.pipeline.context.DataIngestionContext;
import com.demo.outbox.repository.IngestionRecordRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Step 0 — Detokenises the inbound token to recover the PAN needed by every
 * downstream step.
 *
 * BLOCKING_REQUIRED (default): everything after this step needs the
 * detokenised value, so it must succeed before the pipeline proceeds.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class DetokenisationStep implements PipelineStep<DataIngestionContext> {

    public static final String PIPELINE_TYPE = "DATA_INGESTION_PIPELINE";

    // Inject your DetokenisationClient (vault/HSM) here
    // private final DetokenisationClient detokenisationClient;

    private final IngestionRecordRepository ingestionRecordRepository;

    @Override
    public String getPipelineType() { return PIPELINE_TYPE; }

    @Override
    public int getStepIndex() { return 0; }

    @Override
    public Class<DataIngestionContext> getContextClass() { return DataIngestionContext.class; }

    @Override
    public boolean execute(DataIngestionContext ctx) throws Exception {
        log.info("[Step 0] Detokenising recordId={}", ctx.getRecordId());

        // String pan = detokenisationClient.detokenise(ctx.getToken());
        // TODO: replace with real DetokenisationClient call
        String pan = "4111111111111111";
        String last4 = pan.substring(pan.length() - 4);
        ctx.setPanLast4(last4);

        IngestionRecord record = ingestionRecordRepository.findById(ctx.getRecordId())
            .orElseThrow(() -> new IllegalStateException("IngestionRecord not found: " + ctx.getRecordId()));
        record.setPanLast4(last4);
        record.setStatus(IngestionRecord.Status.DETOKENISED);
        ingestionRecordRepository.save(record);

        log.info("[Step 0] Detokenisation complete, panLast4={}", last4);
        return true;
    }
}
