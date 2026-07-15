package com.nodev.notification.model;

import lombok.Getter;
import org.springframework.boot.autoconfigure.graphql.ConditionalOnGraphQlSchema;

import java.time.Instant;

@Getter
public class GenericResponse<T> {
    private final boolean success;
    private final String message;
    private final T data;
    private final Instant timestamp;

    public GenericResponse(boolean success, String message, T data) {
        this.success = success;
        this.message = message;
        this.data = data;
        this.timestamp = Instant.now();
    }
    public static <T> GenericResponse<T> success(String message, T data) {
        return new GenericResponse<>(true, message, data);
    }

    public static <T> GenericResponse<T> failure(String message) {
        return new GenericResponse<>(false, message, null);
    }

}
