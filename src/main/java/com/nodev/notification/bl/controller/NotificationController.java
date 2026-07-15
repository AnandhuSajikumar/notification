package com.nodev.notification.bl.controller;
import com.nodev.notification.bl.entity.NotificationRequest;
import com.nodev.notification.bl.service.NotificationService;
import com.nodev.notification.model.CreateNotificationRequest;
import com.nodev.notification.model.GenericResponse;
import com.nodev.notification.model.NotificationResponse;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/notifications")
@RequiredArgsConstructor
public class NotificationController {

    private final NotificationService notificationService;

    @PostMapping
    public ResponseEntity<GenericResponse<NotificationResponse>> createNotification(
            @Valid @RequestBody CreateNotificationRequest request) {

        NotificationRequest created = notificationService.createNotification(request);

        return ResponseEntity
                .status(HttpStatus.CREATED)
                .body(GenericResponse.success("Notification request accepted", new NotificationResponse(created)));
    }
}