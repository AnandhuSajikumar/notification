package com.nodev.notification.bl.controller;

import com.nodev.notification.bl.entity.NotificationRequest;
import com.nodev.notification.bl.entity.enums.NotificationStatus;
import com.nodev.notification.model.CreateNotificationRequest;
import com.nodev.notification.model.GenericResponse;
import com.nodev.notification.model.NotificationResponse;
import com.nodev.notification.bl.service.DlqService;
import com.nodev.notification.bl.service.NotificationService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/notifications")
@RequiredArgsConstructor
public class NotificationController {

    private final NotificationService notificationService;
    private final DlqService dlqService;

    @PostMapping
    public ResponseEntity<GenericResponse<NotificationResponse>> createNotification(
            @Valid @RequestBody CreateNotificationRequest request) {

        NotificationRequest created = notificationService.createNotification(request);

        return ResponseEntity
                .status(HttpStatus.CREATED)
                .body(GenericResponse.success("Notification request accepted", new NotificationResponse(created)));
    }

    // DLQ inspection: GET /notifications?status=FAILED&page=0&size=20
    @GetMapping
    public ResponseEntity<GenericResponse<Page<NotificationResponse>>> listByStatus(
            @RequestParam NotificationStatus status,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {

        Page<NotificationRequest> results = dlqService.listByStatus(status, PageRequest.of(page, size));
        Page<NotificationResponse> response = results.map(NotificationResponse::new);

        return ResponseEntity.ok(GenericResponse.success("Notifications retrieved", response));
    }

    // Manual replay: POST /notifications/{id}/replay
    // Only valid for FAILED notifications - reuses the exact same retry
    // logic RetryScheduler uses automatically, just admin-triggered.
    @PostMapping("/{id}/replay")
    public ResponseEntity<GenericResponse<Void>> replay(@PathVariable Long id) {
        dlqService.replay(id);
        return ResponseEntity.ok(GenericResponse.success("Replay triggered", null));
    }
}
