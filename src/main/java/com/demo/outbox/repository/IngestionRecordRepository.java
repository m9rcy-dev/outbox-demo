package com.demo.outbox.repository;

import com.demo.outbox.entity.IngestionRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface IngestionRecordRepository extends JpaRepository<IngestionRecord, UUID> {

    Optional<IngestionRecord> findByIdempotencyKey(String idempotencyKey);
}
