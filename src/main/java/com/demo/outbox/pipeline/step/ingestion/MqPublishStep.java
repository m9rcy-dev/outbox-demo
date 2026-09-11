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
 * Step 4 — Publishes an ingestion-complete event to the MQ so downstream
 * systems can react.
 *
 * Input:  recordId, mastercardMatchId (from context)
 * Output: messageId
 *
 * BLOCKING_SELF_RETRIED: the MQ client already has built-in redelivery, so
 * an exception here means the broker attempt is already exhausted — a
 * couple of pipeline-level retries (maxRetries=2) is enough on top. Kept
 * blocking, not fire-and-forget, because this is the final signal
 * downstream systems rely on to know ingestion finished.
 */
@Component("ingestionMqPublishStep")
@RequiredArgsConstructor
@Slf4j
public class MqPublishStep implements PipelineStep<DataIngestionContext> {

    // Inject your MqClient / JmsTemplate / KafkaTemplate here
    // private final MqClient mqClient;

    private final IngestionRecordRepository ingestionRecordRepository;

    @Override
    public String getPipelineType() { return DetokenisationStep.PIPELINE_TYPE; }

    @Override
    public int getStepIndex() { return 4; }

    @Override
    public Class<DataIngestionContext> getContextClass() { return DataIngestionContext.class; }

    @Override
    public StepMode getMode() { return StepMode.BLOCKING_SELF_RETRIED; }

    @Override
    public int getMaxRetries() { return 2; }

    @Override
    public boolean execute(DataIngestionContext ctx) throws Exception {
        log.info("[Step 4] Publishing ingestion-complete for recordId={}", ctx.getRecordId());

        // String messageId = mqClient.publish("ingestion.completed", Map.of(
        //     "recordId", ctx.getRecordId(),
        //     "mastercardMatchId", ctx.getMastercardMatchId()
        // ));
        // TODO: replace with real MQ publish
        ctx.setMessageId("MSG-" + ctx.getRecordId());

        IngestionRecord record = ingestionRecordRepository.findById(ctx.getRecordId())
            .orElseThrow(() -> new IllegalStateException("IngestionRecord not found: " + ctx.getRecordId()));
        record.setStatus(IngestionRecord.Status.INGESTED);
        ingestionRecordRepository.save(record);

        log.info("[Step 4] MQ message published, id={}", ctx.getMessageId());
        return true;
    }
}
