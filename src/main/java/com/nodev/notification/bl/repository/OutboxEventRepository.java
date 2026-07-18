package com.nodev.notification.bl.repository;

import com.nodev.notification.bl.entity.OutboxEvent;
import org.hibernate.sql.ast.tree.expression.JdbcParameter;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.PagingAndSortingRepository;


public interface OutboxEventRepository extends JpaRepository<OutboxEvent, Long> {
    // The outbox poller's main query. Bounded via Pageable so a single poll
    // cycle never tries to pull an unlimited number of rows if the poller
    // has fallen behind (e.g. Kafka was down for a while) — matches the
    // partial index idx_outbox_events_unpublished in V3.
    Page<OutboxEvent> findByPublishedFalseOrderByCreatedAtAsc(Pageable pageable);

    @Query(
            value = "SELECT * FROM outbox_events WHERE published = false ORDER BY created_at ASC",
            countQuery = "SELECT COUNT(*) FROM outbox_events WHERE published = false",
            nativeQuery = true
    )

    Page<OutboxEvent> findUnPublishedBatch(Pageable pageable);

}
