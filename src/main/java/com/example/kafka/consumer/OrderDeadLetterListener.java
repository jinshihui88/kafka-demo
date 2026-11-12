package com.example.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * 死信 Topic 监听器：开发环境用于观察进入 DLT 的消息；
 * 生产环境应替换为 DLT 监控告警 + 人工核查 + 修复重放的运维流程。
 */
@Component
public class OrderDeadLetterListener {

    private static final Logger log = LoggerFactory.getLogger(OrderDeadLetterListener.class);

    @KafkaListener(
            topics = "${app.kafka.topics.order-created-dlt}",
            groupId = "${spring.kafka.consumer.group-id}")
    public void onDeadLetter(ConsumerRecord<String, String> record) {
        log.error("收到死信消息：topic={}, partition={}, offset={}, key={}, 原始消息={}",
                record.topic(), record.partition(), record.offset(), record.key(), record.value());
        log.error("死信详情：原始Topic={}, 原始分区={}, 原始Offset={}, 异常类型={}, 异常信息={}",
                stringHeader(record.headers(), KafkaHeaders.DLT_ORIGINAL_TOPIC),
                intHeader(record.headers(), KafkaHeaders.DLT_ORIGINAL_PARTITION),
                longHeader(record.headers(), KafkaHeaders.DLT_ORIGINAL_OFFSET),
                stringHeader(record.headers(), KafkaHeaders.DLT_EXCEPTION_FQCN),
                stringHeader(record.headers(), KafkaHeaders.DLT_EXCEPTION_MESSAGE));
    }

    private String stringHeader(Headers headers, String key) {
        Header header = headers.lastHeader(key);
        return header == null || header.value() == null
                ? null
                : new String(header.value(), StandardCharsets.UTF_8);
    }

    /** DLT_ORIGINAL_PARTITION 头以 int 二进制存储，需按 int 解码。 */
    private String intHeader(Headers headers, String key) {
        return Optional.ofNullable(headers.lastHeader(key))
                .map(Header::value)
                .filter(value -> value.length >= Integer.BYTES)
                .map(value -> String.valueOf(ByteBuffer.wrap(value).getInt()))
                .orElse(null);
    }

    /** DLT_ORIGINAL_OFFSET 头以 long 二进制存储，需按 long 解码。 */
    private String longHeader(Headers headers, String key) {
        return Optional.ofNullable(headers.lastHeader(key))
                .map(Header::value)
                .filter(value -> value.length >= Long.BYTES)
                .map(value -> String.valueOf(ByteBuffer.wrap(value).getLong()))
                .orElse(null);
    }
}
