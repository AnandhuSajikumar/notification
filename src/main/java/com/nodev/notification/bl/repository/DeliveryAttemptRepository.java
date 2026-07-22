package com.nodev.notification.bl.repository;

import com.nodev.notification.bl.entity.DeliveryAttempt;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface DeliveryAttemptRepository extends JpaRepository<DeliveryAttempt, Long> {

    // Simple single-relation traversal, one reasonable query — derived
    // method name is fine here, nothing ambiguous to hide.
    // Reconstructs the full "story" of a notification: first try, any
    // retries, any fallback — in order, by walking attempt_number.
    List<DeliveryAttempt> findByNotification_IdOrderByAttemptNumberAsc(Long notificationId);

    // Used by replay: the most recent attempt is what a manual replay
    // retries from, same as RetryScheduler retrying the latest failure.
    Optional<DeliveryAttempt> findTopByNotification_IdOrderByAttemptNumberDesc(Long notificationId);

    // The Week-2 retry scheduler's core query — explicit native query so
    // the SQL is visible, and 'FAILURE' is a hardcoded literal (not a bind
    // parameter) to exactly match idx_delivery_attempts_retry_due, a
    // partial index defined as WHERE result = 'FAILURE' AND next_retry_at
    // IS NOT NULL. Parameterizing result here would risk the planner not
    // recognizing it can use the partial index at plan time.
    @Query(
        value = "SELECT * FROM delivery_attempts " +
                "WHERE result = 'FAILURE' AND next_retry_at IS NOT NULL AND next_retry_at <= :now " +
                "ORDER BY next_retry_at ASC",
        countQuery = "SELECT count(*) FROM delivery_attempts " +
                "WHERE result = 'FAILURE' AND next_retry_at IS NOT NULL AND next_retry_at <= :now",
        nativeQuery = true
    )
    Page<DeliveryAttempt> findRetryDueAttempts(@Param("now") Instant now, Pageable pageable);
}
