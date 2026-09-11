package com.demo.outbox.pipeline.step.token;

import com.demo.outbox.pipeline.PipelineStep;
import com.demo.outbox.pipeline.StepMode;
import com.demo.outbox.pipeline.context.TokenUpdateContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Step 2 — Publishes a token-status-changed event to the MQ so downstream
 * systems (fraud, rewards, mobile-app) can react asynchronously.
 *
 * Input:  cardId, updatedStatus, mastercardRef (from context)
 * Output: messageId (set on context)
 *
 * BLOCKING_SELF_RETRIED: the MQ client has its own built-in redelivery /
 * backoff, so a thrown exception here already means the broker attempt was
 * exhausted — a couple of pipeline-level retries is enough on top. Kept
 * blocking (not fire-and-forget) because downstream consumers (fraud,
 * rewards, mobile-app) depend on this message actually landing.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class MqPublishStep implements PipelineStep<TokenUpdateContext> {

    // Inject your MqClient / JmsTemplate / KafkaTemplate here
    // private final MqClient mqClient;

    @Override
    public String getPipelineType() { return MastercardTokenUpdateStep.PIPELINE_TYPE; }

    @Override
    public int getStepIndex() { return 2; }

    @Override
    public Class<TokenUpdateContext> getContextClass() { return TokenUpdateContext.class; }

    @Override
    public StepMode getMode() { return StepMode.BLOCKING_SELF_RETRIED; }

    @Override
    public int getMaxRetries() { return 2; }

    @Override
    public boolean execute(TokenUpdateContext ctx) throws Exception {
        log.info("[Step 2] Publishing token status change for cardId={}", ctx.getCardId());

        // String messageId = mqClient.publish("token.status.changed", Map.of(
        //     "cardId", ctx.getCardId(),
        //     "status", ctx.getUpdatedStatus(),
        //     "mastercardRef", ctx.getMastercardRef()
        // ));
        // ctx.setMessageId(messageId);

        // TODO: replace with real MQ publish
        ctx.setMessageId("MSG-" + ctx.getCardId());
        log.info("[Step 2] MQ message published id={}", ctx.getMessageId());
        return true;
    }
}
