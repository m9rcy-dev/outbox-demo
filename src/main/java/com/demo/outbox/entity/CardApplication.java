package com.demo.outbox.entity;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "card_application")
@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class CardApplication {

    public enum Status {
        SUBMITTED,          // initial state
        CREDIT_CHECKED,     // step 0 done
        PROVIDER_REGISTERED,// step 1 done
        NOTIFIED,           // step 2 done
        COMPLETED,          // all done
        FAILED              // pipeline permanently failed
    }

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(nullable = false)
    private String applicantName;

    @Column(nullable = false)
    private String email;

    @Column(nullable = false)
    private BigDecimal annualIncome;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private Status status = Status.SUBMITTED;

    // Populated progressively by pipeline steps
    private Integer creditScore;
    private String providerRef;
    private String notificationId;

    @CreationTimestamp
    private LocalDateTime createdAt;

    @UpdateTimestamp
    private LocalDateTime updatedAt;
}
