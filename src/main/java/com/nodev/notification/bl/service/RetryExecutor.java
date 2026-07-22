package com.nodev.notification.bl.service;


import com.nodev.notification.bl.RetryPolicy;
import com.nodev.notification.bl.entity.DeliveryAttempt;
import com.nodev.notification.bl.entity.NotificationRequest;
import com.nodev.notification.bl.entity.enums.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class RetryExecutor {
    private final SubscriberResolver subscriberResolver;
    private final RetryPolicy retryPolicy;
    private final DispatchService dispatchService;
    private final RetryOutcomeRecorder retryOutcomeRecorder;

    public void retryOne(DeliveryAttempt failedAttempt){
        NotificationRequest notificationRequest = failedAttempt.getNotification();
        Channel channel =  failedAttempt.getChannel();
        int newAttemptNumber = failedAttempt.getAttemptNumber() + 1;

        String resolvedAddress =  subscriberResolver.resolve(notificationRequest.getRecipientId(), channel);

        boolean success = resolvedAddress != null
                && dispatchService.dispatch(channel, resolvedAddress,notificationRequest.getPayload());

        DeliveryAttempt retryAttempt =  new DeliveryAttempt(
                notificationRequest, failedAttempt, channel, newAttemptNumber
        );

        if(success){
            retryAttempt.markSuccess();
            notificationRequest.markSent();
        }else if(
            retryPolicy.isExhausted(newAttemptNumber)){
                retryAttempt.markFailure(null);
                notificationRequest.markFailed();
                log.warn("Notification id={} exhausted retries after {} attempts, dead-lettered",
                        notificationRequest.getId(), newAttemptNumber);
            }
        else{
            retryAttempt.markFailure(retryPolicy.nextRetryAt(newAttemptNumber));
        }
        failedAttempt.markRetryScheduled();
        retryOutcomeRecorder.record(failedAttempt, retryAttempt, notificationRequest);

        log.info("Notification id={} attempt #{} on channel={} result={}",
                notificationRequest.getId(), newAttemptNumber, channel, success ? "SUCCESS" : "FAILURE");

    }
}
