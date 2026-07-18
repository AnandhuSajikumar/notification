package com.nodev.notification.bl.repository;

import com.nodev.notification.bl.entity.DeliveryAttempt;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.PagingAndSortingRepository;

import java.time.Instant;
import java.util.List;

public interface DeliveryAttemptRepository extends JpaRepository<DeliveryAttempt, Long> {

    List<DeliveryAttempt> findByNotification_IdOrderByAttemptNumberAsc(Long notificationId);


    @Query (
            value = "SELECT * FROM delivery_attempts " +
                    "WHERE result = 'FAILURE' AND next_retry_at IS NOT NULL AND next_retry_at <= :now " +
                    "ORDER BY next_retry_at ASC",
        countQuery = "SELECT count(*) FROM delivery_attempts" +
                    "WHERE result = 'FAILURE' AND next_retry_at IS NOT NULL AND next_retry_at <= :now ",
            nativeQuery = true
    )
    Page<DeliveryAttempt> findRetryDueAttempts(Instant now, Pageable pageable);

}
