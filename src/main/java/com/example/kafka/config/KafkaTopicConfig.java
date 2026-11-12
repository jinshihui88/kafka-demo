package com.example.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic orderCreatedTopic(
            @Value("${app.kafka.topics.order-created}") String topicName) {
        // 本地单 Broker 示例：副本数只能为 1。
        // 生产集群应按 Broker 数量与可用性目标调整副本数。
        return TopicBuilder.name(topicName)
                .partitions(6)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic orderCreatedDltTopic(
            @Value("${app.kafka.topics.order-created-dlt}") String topicName) {
        // 失败消息按"同分区号"转发到 DLT，DLT 分区数至少应与源 Topic 一致。
        return TopicBuilder.name(topicName)
                .partitions(6)
                .replicas(1)
                .build();
    }
}
