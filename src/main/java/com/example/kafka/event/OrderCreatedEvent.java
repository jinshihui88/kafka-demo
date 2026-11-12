package com.example.kafka.event;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 订单创建事件。eventId 是事件唯一标识；orderId 作为 Kafka Key，
 * 保证同一订单的事件进入同一分区，维持该订单维度的顺序。
 */
public record OrderCreatedEvent(
        String eventId,
        String orderId,
        Long userId,
        BigDecimal amount,
        LocalDateTime occurredAt
) {
}
