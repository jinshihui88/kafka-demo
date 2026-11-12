package com.example.kafka;

import com.example.kafka.consumer.OrderCreatedListener;
import com.example.kafka.event.OrderCreatedEvent;
import com.example.kafka.producer.OrderEventProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

/**
 * 端到端集成测试：使用嵌入式 Kafka Broker，
 * 验证「生产者发布 → Broker → 消费者监听器收到消息」的完整链路。
 */
@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        topics = {"order-created", "order-created-dlt"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class OrderKafkaIntegrationTest {

    @Autowired
    private OrderEventProducer producer;

    @MockitoSpyBean
    private OrderCreatedListener listener;

    @Test
    void publishShouldReachListener() {
        var event = new OrderCreatedEvent(
                UUID.randomUUID().toString(),
                "ORDER-TEST-1",
                1001L,
                new BigDecimal("99.90"),
                LocalDateTime.now());

        producer.publish(event).join();

        // 等待监听器被真实调用（默认 10 秒超时，避免使用 Thread.sleep）
        verify(listener, timeout(10_000)).onMessage(any());
    }
}
