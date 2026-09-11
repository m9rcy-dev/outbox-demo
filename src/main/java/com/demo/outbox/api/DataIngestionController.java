package com.demo.outbox.api;

import com.demo.outbox.entity.IngestionRecord;
import com.demo.outbox.service.DataIngestionService;
import com.demo.outbox.util.MdcContext;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/ingestion-records")
@RequiredArgsConstructor
@Slf4j
public class DataIngestionController {

    private final DataIngestionService dataIngestionService;

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public IngestionRecord submit(@Valid @RequestBody SubmitRequest request,
                                   @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
                                   HttpServletRequest httpRequest) {
        try (MdcContext mdc = MdcContext.forHttpRequest(httpRequest.getHeader("X-Request-ID"))) {
            log.info("Submitting ingestion record, idempotencyKey={}", idempotencyKey);
            IngestionRecord result = dataIngestionService.submitRecord(
                request.getToken(),
                request.getCardholderPhone(),
                idempotencyKey
            );
            log.info("Ingestion record submitted, id={}", result.getId());
            return result;
        }
    }

    @GetMapping("/{id}")
    public IngestionRecord get(@PathVariable UUID id, HttpServletRequest httpRequest) {
        try (MdcContext mdc = MdcContext.forHttpRequest(httpRequest.getHeader("X-Request-ID"))) {
            log.info("Fetching ingestion record id={}", id);
            return dataIngestionService.getRecord(id);
        }
    }

    @Data
    public static class SubmitRequest {
        @NotBlank
        private String token;

        @NotBlank
        private String cardholderPhone;
    }
}
