package com.example.kafka.consumer;

import com.example.kafka.event.OrderCreatedEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderCreatedListener {

    private static final Logger log = LoggerFactory.getLogger(OrderCreatedListener.class);

    private final ObjectMapper objectMapper;

    public OrderCreatedListener(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.order-created}",
            groupId = "${spring.kafka.consumer.group-id}")
    public void onMessage(ConsumerRecord<String, String> record) {
        try {
            OrderCreatedEvent event = objectMapper.readValue(
                    record.value(), OrderCreatedEvent.class);

            // TODO：调用业务 Service 完成订单后续处理。
            // 应以 event.eventId() 或业务唯一键做幂等控制。

            log.info("收到订单事件：eventId={}, orderId={}, userId={}, amount={}, partition={}, offset={}",
                    event.eventId(), event.orderId(), event.userId(), event.amount(),
                    record.partition(), record.offset());

        } catch (JsonProcessingException ex) {
            // 转为不可重试异常，由错误处理器直接送入 DLT
            throw new IllegalArgumentException("订单事件 JSON 格式不正确", ex);
        }
    }
}
