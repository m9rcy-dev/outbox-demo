package com.demo.outbox.api;

import com.demo.outbox.entity.CardApplication;
import com.demo.outbox.service.CardApplicationService;
import com.demo.outbox.util.MdcContext;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/card-applications")
@RequiredArgsConstructor
@Slf4j
public class CardApplicationController {

    private final CardApplicationService cardApplicationService;

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public CardApplication submit(@Valid @RequestBody SubmitRequest request,
                                  HttpServletRequest httpRequest) {
        try (MdcContext mdc = MdcContext.forHttpRequest(httpRequest.getHeader("X-Request-ID"))) {
            log.info("Submitting card application for applicant={}", request.getApplicantName());
            CardApplication result = cardApplicationService.submitApplication(
                request.getApplicantName(),
                request.getEmail(),
                request.getAnnualIncome()
            );
            log.info("Card application submitted, id={}", result.getId());
            return result;
        }
    }

    @GetMapping("/{id}")
    public CardApplication get(@PathVariable UUID id, HttpServletRequest httpRequest) {
        try (MdcContext mdc = MdcContext.forHttpRequest(httpRequest.getHeader("X-Request-ID"))) {
            log.info("Fetching card application id={}", id);
            return cardApplicationService.getApplication(id);
        }
    }

    @Data
    public static class SubmitRequest {
        @NotBlank
        private String applicantName;

        @Email @NotBlank
        private String email;

        @Positive
        private BigDecimal annualIncome;
    }
}
