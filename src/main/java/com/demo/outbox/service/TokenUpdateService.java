package com.demo.outbox.service;

import com.demo.outbox.pipeline.context.TokenUpdateContext;
import com.demo.outbox.pipeline.step.token.MastercardTokenUpdateStep;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * Entry point for the TOKEN_UPDATE_PIPELINE saga.
 *
 * The business entity update and the outbox event are saved in one transaction:
 * if either write fails, both roll back — the saga never starts in a half-created state.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class TokenUpdateService {

    private final OutboxService outboxService;
    // Inject your CardTokenRepository here to do the initial business write
    // private final CardTokenRepository cardTokenRepository;

    @Transactional
    public void requestTokenStatusUpdate(UUID cardId, String tokenId,
                                         String desiredStatus, String cardholderPhone) {
        // Step A: update business entity to PENDING_UPDATE (optional — shows intent)
        // CardToken token = cardTokenRepository.findById(cardId).orElseThrow();
        // token.setStatus("PENDING_UPDATE");
        // cardTokenRepository.save(token);

        // Step B: enqueue the saga — atomic with Step A
        TokenUpdateContext context = TokenUpdateContext.builder()
            .cardId(cardId)
            .tokenId(tokenId)
            .desiredStatus(desiredStatus)
            .cardholderPhone(cardholderPhone)
            .build();

        outboxService.saveEvent(
            MastercardTokenUpdateStep.PIPELINE_TYPE,
            cardId,
            context
        );

        log.info("Token update saga queued for cardId={} tokenId={} desiredStatus={}",
            cardId, tokenId, desiredStatus);
    }
}
