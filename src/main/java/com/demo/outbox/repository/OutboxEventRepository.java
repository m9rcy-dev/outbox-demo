package com.demo.outbox.repository;

import com.demo.outbox.entity.OutboxEvent;
import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Repository
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    /**
     * Fetch PENDING events in creation order.
     *
     * PESSIMISTIC_WRITE generates SELECT FOR UPDATE in PostgreSQL.
     * SKIP_LOCKED (-2) tells PostgreSQL to skip rows already locked by another pod,
     * so each pod works on a distinct subset of the batch rather than blocking.
     *
     * The lock is held only for the duration of this short transaction. Events are
     * re-fetched and rechecked inside OutboxEventProcessor.process() before any work
     * starts, so this fetch acts as a coarse distribution hint, not the primary
     * concurrency guard. The @Version field on OutboxEvent is the hard guard.
     */
    @Transactional
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = "-2"))
    @Query(value = """
        SELECT e FROM OutboxEvent e
        WHERE e.status = 'PENDING'
        ORDER BY e.createdAt ASC
        LIMIT :limit
    """)
    List<OutboxEvent> findPendingForProcessing(@Param("limit") int limit);

    List<OutboxEvent> findByCorrelationId(UUID correlationId);
}
