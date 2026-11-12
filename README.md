# kafka-demo

Spring Boot 3 整合 Apache Kafka 3.9.2 的完整示例工程：订单创建事件的发布与消费、有序性保障、消费异常重试与死信队列（DLT）、集成测试。

项目代码已在本地真实编译、启动并与 Kafka 3.9.2 Broker 收发验证通过。

## 技术栈

| 组件 | 版本 | 说明 |
| --- | --- | --- |
| JDK | 17 | 运行与编译基线 |
| Spring Boot | 3.5.16 | 父 POM 统一管理依赖版本 |
| Spring for Apache Kafka | 3.3.16 | 由 Spring Boot BOM 管理，无需手写版本 |
| Apache Kafka（Broker） | 3.9.2 | KRaft 模式单节点，需自行启动 |
| kafka-clients | 3.9.2 | 由 Spring Boot BOM 管理 |
| Maven | 3.6.3+ | 构建工具 |

## 目录结构

```text
kafka-demo
├─ src/main/java/com/example/kafka
│  ├─ KafkaDemoApplication.java          启动类
│  ├─ config
│  │  ├─ KafkaTopicConfig.java           Topic 声明（order-created / order-created-dlt，各 6 分区）
│  │  └─ KafkaErrorConfig.java           消费异常重试 + 死信转发
│  ├─ controller
│  │  └─ OrderEventController.java       联调用消息投递接口
│  ├─ event
│  │  └─ OrderCreatedEvent.java          订单创建事件（record）
│  ├─ producer
│  │  └─ OrderEventProducer.java         KafkaTemplate 异步发送
│  └─ consumer
│     ├─ OrderCreatedListener.java       业务消费监听器
│     └─ OrderDeadLetterListener.java    死信 Topic 监听器（含 DLT 头解析）
├─ src/main/resources/application.yml    生产者/消费者/监听容器配置
├─ src/test/java/com/example/kafka
│  └─ OrderKafkaIntegrationTest.java     嵌入式 Kafka 端到端测试
├─ docs/SpringBoot3整合Kafka3.9实战教程.md  配套实战教程
└─ tools/                                md 转公众号 Word 稿脚本与模板
```

## 前置条件

1. **JDK 17**，`JAVA_HOME` 指向 JDK 17；
2. **Kafka 3.9.2** 已启动并监听 `localhost:9092`（KRaft 模式，首次需 `kafka-storage.bat format`）；
3. 本机 **8090 端口**空闲（8080 常被其他服务占用，故本示例使用 8090）。

确认 Kafka 可用：

```bat
bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --list
```

## 快速开始

```bash
# 构建（含集成测试，测试自带嵌入式 Broker，不依赖本地 Kafka）
mvn clean package

# 启动
java -jar target/kafka-demo-1.0.0.jar
```

Windows 上若 PATH 中仍是 JDK 8，请使用完整路径启动，并建议加 UTF-8 参数避免控制台中文乱码：

```bat
"C:\Program Files\Java\jdk-17\bin\java.exe" -Dfile.encoding=UTF-8 -jar target\kafka-demo-1.0.0.jar
```

启动成功后可用健康检查确认：

```text
GET http://127.0.0.1:8090/actuator/health
{"status":"UP"}
```

## 接口说明

`POST /test/orders/events`：投递一条订单创建事件（仅用于开发联调，生产环境不应开放）。

```bash
curl -X POST "http://127.0.0.1:8090/test/orders/events" \
  -H "Content-Type: application/json" \
  -d "{\"orderId\":\"ORDER-10001\",\"userId\":1001,\"amount\":\"99.90\"}"
```

成功返回 HTTP 202，响应体包含实际落库的 Topic、分区与 Offset：

```text
消息已发送：topic=order-created, partition=5, offset=0
```

参数缺失或非法返回 400；Kafka 发送失败返回 502 并带上 eventId 便于排查。

## 关键行为

- **Key 保序**：`orderId` 作为消息 Key，同一订单的事件稳定进入同一分区，分区内有序；
- **手动提交**：关闭客户端自动提交，`ack-mode: record` 由监听容器在单条处理成功后提交 Offset；
- **异常分级**：可重试异常按 `FixedBackOff(1s, 3次)` 重试；`IllegalArgumentException`（如 JSON 格式错误）视为不可重试，直接转发死信；
- **死信同分区**：失败消息进入 `order-created-dlt` 的同编号分区，故 DLT 分区数不小于源 Topic；
- **消息幂等**：生产端开启 `enable.idempotence`，消费端业务幂等需在业务代码中以 `eventId` 实现（示例中留了 TODO 位置）。

## 验证要点

```bat
REM 查看 Topic
bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --list

REM 查看消费组 Lag（正常应全为 0）
bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --describe --group order-service-group
```

投递一条非法 JSON 可验证死信链路：

```bat
echo {"brokenJson": | bin\windows\kafka-console-producer.bat --bootstrap-server localhost:9092 --topic order-created
```

应用日志会打印死信消息及其原始 Topic、分区、Offset、异常类型。

## 常见问题

- **Windows 上执行 Kafka 脚本报「输入行太长。命令语法不正确。」**
  与 JDK 版本无关，原因是 `kafka-run-class.bat` 枚举全部 jar 拼接的 java 命令超过 CMD 单行 8191 字符上限。把该脚本中的 `libs` 枚举循环替换为 `set CLASSPATH="%BASE_DIR%\libs\*"` 即可，详见配套教程第二节。
- **`/tmp/kraft-combined-logs` 找不到数据目录**
  Windows 下该路径解析为「当前盘符:\tmp」，例如从 D 盘启动就落在 `D:\tmp\kraft-combined-logs`。
- **DLT 日志中原始分区/Offset 显示空白**
  `DLT_ORIGINAL_PARTITION` 是 int、`DLT_ORIGINAL_OFFSET` 是 long 二进制头，必须用 `ByteBuffer` 解码，不能按字符串处理。

## 配套文档

- 实战教程（含 KRaft 启动、完整踩坑过程与实测日志）：[docs/SpringBoot3整合Kafka3.9实战教程.md](docs/SpringBoot3整合Kafka3.9实战教程.md)
- 公众号 Word 稿：`SpringBoot3整合Kafka3.9实战教程.docx`

## 生产环境提醒

本项目是开发联调示例，直接上生产还需补齐：消费幂等（`eventId` + 唯一索引，与业务更新同一事务）、数据库与 Kafka 双写的 Outbox 方案、Consumer Lag 与 DLT 监控告警、Topic/ACL 统一管控、TLS/SASL 认证，以及基于压测的参数调优。单机单副本配置不具备高可用能力。
