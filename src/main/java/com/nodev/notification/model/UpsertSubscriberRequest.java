package com.nodev.notification.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class UpsertSubscriberRequest {
    private String email;
    private String phone;
    private String pushToken;
}
