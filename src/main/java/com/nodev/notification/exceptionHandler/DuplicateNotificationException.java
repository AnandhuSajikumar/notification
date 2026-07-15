package com.nodev.notification.exceptionHandler;


public class DuplicateNotificationException extends RuntimeException{
     public DuplicateNotificationException(String idempotencyKey){
         super("A notification with idempotencyKey '" + idempotencyKey + "' already exists");
     }
}
