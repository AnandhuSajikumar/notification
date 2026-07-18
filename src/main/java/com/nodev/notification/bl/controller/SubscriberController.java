package com.nodev.notification.bl.controller;

import com.nodev.notification.bl.model.GenericResponse;
import com.nodev.notification.bl.model.UpsertSubscriberRequest;
import com.nodev.notification.bl.service.SubscriberService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/subscribers")
@RequiredArgsConstructor
public class SubscriberController {

    private final SubscriberService subscriberService;

    // PUT, not POST: idempotent by design - calling this again for the
    // same recipientId updates the record rather than creating a duplicate
    // or erroring, matching the "identify" pattern's semantics.
    @PutMapping("/{recipientId}")
    public ResponseEntity<GenericResponse<Void>> upsert(
            @PathVariable String recipientId,
            @Valid @RequestBody UpsertSubscriberRequest request) {

        subscriberService.upsert(recipientId, request);

        return ResponseEntity
                .status(HttpStatus.OK)
                .body(GenericResponse.success("Subscriber synced", null));
    }
}
