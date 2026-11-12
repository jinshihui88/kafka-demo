package com.example.kafka.producer;

import com.example.kafka.event.OrderCreatedEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class OrderEventProducer {

    private final KafkaTemplate<Object, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topic;

    public OrderEventProducer(
            KafkaTemplate<Object, Object> kafkaTemplate,
            ObjectMapper objectMapper,
            @Value("${app.kafka.topics.order-created}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topic = topic;
    }

    /**
     * 发布订单创建事件。
     * 返回的 CompletableFuture 仅代表"客户端已接受发送任务"，
     * 业务确认发送成功必须在 future 正常完成后处理。
     */
    public CompletableFuture<SendResult<Object, Object>> publish(OrderCreatedEvent event) {
        try {
            String json = objectMapper.writeValueAsString(event);
            // orderId 作为消息 Key，保证同一订单事件进入同一分区
            return kafkaTemplate.send(topic, event.orderId(), json);
        } catch (JsonProcessingException ex) {
            throw new IllegalArgumentException("订单事件 JSON 序列化失败", ex);
        }
    }
}
