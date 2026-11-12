package com.example.kafka.controller;

import com.example.kafka.event.OrderCreatedEvent;
import com.example.kafka.producer.OrderEventProducer;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * 开发联调用的测试接口。生产环境不应开放无权限的消息投递入口。
 */
@RestController
@RequestMapping("/test/orders")
public class OrderEventController {

    private static final Logger log = LoggerFactory.getLogger(OrderEventController.class);

    private final OrderEventProducer producer;

    public OrderEventController(OrderEventProducer producer) {
        this.producer = producer;
    }

    @PostMapping("/events")
    public CompletableFuture<ResponseEntity<String>> publish(
            @Valid @RequestBody CreateOrderEventRequest request) {
        var event = new OrderCreatedEvent(
                UUID.randomUUID().toString(), request.orderId(),
                request.userId(), request.amount(), LocalDateTime.now());

        return producer.publish(event)
                .thenApply(result -> {
                    var metadata = result.getRecordMetadata();
                    return ResponseEntity.accepted().body(
                            "消息已发送：topic=%s, partition=%d, offset=%d"
                                    .formatted(metadata.topic(), metadata.partition(), metadata.offset()));
                })
                .exceptionally(ex -> {
                    log.error("Kafka 发送失败 eventId={}", event.eventId(), ex.getCause());
                    return ResponseEntity.status(HttpStatus.BAD_GATEWAY)
                            .body("Kafka 发送失败，eventId=" + event.eventId());
                });
    }

    public record CreateOrderEventRequest(
            @NotBlank String orderId,
            @NotNull Long userId,
            @NotNull @Positive BigDecimal amount) {
    }
}
