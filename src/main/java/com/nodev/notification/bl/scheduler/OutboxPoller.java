package com.nodev.notification.bl.scheduler;

import com.nodev.notification.bl.entity.OutboxEvent;
import com.nodev.notification.bl.repository.OutboxEventRepository;
import jakarta.validation.Payload;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.sql.Time;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
@Slf4j
@RequiredArgsConstructor
public class OutboxPoller {

    private static final int BATCH_SIZE = 50;
    private static final long KAFKA_ATK_TIMEOUT_SECONDS = 10;

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${notification.kafka.outbound-topic}")
    private String outboundTopic;

    @Scheduled(fixedDelay = 5000)
    @SchedulerLock(name = "pollAndPublish", lockAtMostFor = "30s", lockAtLeastFor = "4s")
    public void pollAndPublish(){
        Page<OutboxEvent> batch = outboxEventRepository.findUnPublishedBatch(PageRequest.of(0,BATCH_SIZE));
        if(batch.isEmpty()){
            return;
        }

        List<OutboxEvent> events = batch.getContent();
        List<CompletableFuture<SendResult<String, Object>>> futures = new ArrayList<>(events.size());

        // Fire all sends concurrently — Kafka's producer batches these internally
        for (OutboxEvent event : events) {
            futures.add(kafkaTemplate.send(outboundTopic, event.getAggregateId().toString(), event.getPayload()));
        }

        // Await all results — most futures will already be complete by now,
        // total wait ≈ max(individual ack times) instead of sum
        for (int i = 0; i < futures.size(); i++) {
            OutboxEvent event = events.get(i);
            try {
                futures.get(i).get(KAFKA_ATK_TIMEOUT_SECONDS, TimeUnit.SECONDS);

                event.markPublished();
                outboxEventRepository.save(event);

                log.info("Published outbox event id={} type={} to topic={}",
                        event.getId(), event.getEventType(), outboundTopic);

            } catch (ExecutionException | TimeoutException e) {
                log.error("Failed to publish outbox event id={}, will retry next cycle", event.getId(), e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Interrupted while publishing outbox event id={}", event.getId(), e);
            }
        }
    }
}