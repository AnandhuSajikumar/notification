package com.nodev.notification.bl.repository;

import com.nodev.notification.bl.entity.NotificationRequest;
import com.nodev.notification.bl.entity.enums.NotificationStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.PagingAndSortingRepository;

import java.util.Optional;

public interface NotificationRequestRepository
        extends JpaRepository<NotificationRequest, Long>,
        JpaSpecificationExecutor<NotificationRequest>{

    // Used by the POST /notifications endpoint to check for a duplicate
    // request before inserting (idempotency guard at the API layer).
    Optional<NotificationRequest> findByIdempotencyKey(String idempotencyKey);

    // Backs the future admin/DLQ-inspection endpoint (Week 2) — paginated
    // on purpose, since an unbounded findAll() on a FAILED filter would grow
    // unbounded as the system runs.
    Page<NotificationRequest> findByStatus(NotificationStatus status, Pageable pageable);
}