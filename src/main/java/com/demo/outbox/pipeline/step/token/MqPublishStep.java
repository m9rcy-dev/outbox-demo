package com.demo.outbox.pipeline.step.token;

import com.demo.outbox.pipeline.PipelineStep;
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
