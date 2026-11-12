package com.example.kafka.config;

import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaErrorConfig {

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(
            KafkaTemplate<Object, Object> kafkaTemplate) {

        // 失败消息转发到「源Topic名-dlt」的同编号分区
        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(kafkaTemplate,
                        (record, exception) -> new TopicPartition(
                                record.topic() + "-dlt", record.partition()));

        // 首次消费失败后，每隔 1 秒重试 3 次；之后转发到 DLT
        DefaultErrorHandler handler = new DefaultErrorHandler(
                recoverer, new FixedBackOff(1_000L, 3L));

        // 参数/格式错误通常属于不可重试异常，直接进入 DLT
        handler.addNotRetryableExceptions(IllegalArgumentException.class);
        return handler;
    }
}
