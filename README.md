MyBatis Dynamic Query
=====================================

[![License](http://img.shields.io/:license-apache-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
[![Coverage Status](https://coveralls.io/repos/github/wz2cool/local-queue/badge.svg?branch=master)](https://coveralls.io/github/wz2cool/local-queue?branch=master)
[![Maven central](https://maven-badges.herokuapp.com/maven-central/com.github.wz2cool/local-queue/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.wz2cool/local-queue)

### README

# Local Queue - 简单的本地队列系统

## 项目简介

Local Queue 是一个基于 Chronicle Queue 实现的简单本地消息队列系统，支持生产者-消费者模式。它提供了高性能、持久化的消息存储和读取功能，适用于需要在本地环境中进行高效消息传递的应用场景。

## 目录结构

```
local-queue
│── src
│   └── main
│       └── java
│           └── com.github.wz2cool.localqueue
│               ├── impl
│               │   ├── SimpleProducer.java
│               │   └── SimpleConsumer.java
│               ├── model
│               │   ├── config
│               │   │   ├── SimpleProducerConfig.java
│               │   │   └── SimpleConsumerConfig.java
│               │   └── message
│               │       └── QueueMessage.java
│               └── IProducer.java
│               └── IConsumer.java
└── README.md
```


## 主要模块

### 生产者 (Producer)

`SimpleProducer.java` 实现了 `IProducer` 接口，负责将消息写入到 Chronicle Queue 中，并提供批量写入、定时清理旧文件等功能。

#### 关键特性
- **配置管理**：通过 `SimpleProducerConfig` 类进行配置。
- **消息缓存**：使用 `LinkedBlockingQueue` 缓存待写入的消息。
- **批量写入**：支持批量写入以提高性能。
- **定时任务**：定期清理过期的日志文件（根据配置的保留天数）。
- **线程安全**：使用锁机制确保多线程环境下的数据一致性。

### 消费者 (Consumer)

`SimpleConsumer.java` 实现了 `IConsumer` 接口，负责从 Chronicle Queue 中读取消息，并提供多种读取方式（如单条读取、批量读取等）。

#### 关键特性
- **配置管理**：通过 `SimpleConsumerConfig` 类进行配置。
- **消息缓存**：使用 `LinkedBlockingQueue` 缓存已读取但未确认的消息。
- **批量读取**：支持批量读取以提高性能。
- **位置管理**：记录已读取的位置，支持断点续传。
- **定时任务**：定期刷新已确认读取的位置。
- **线程安全**：使用锁机制确保多线程环境下的数据一致性。

## 使用方法

### 配置生产者

```java
File dataDir = new File("/path/to/data");
int keepDays = 7; // 保留7天的数据
int flushBatchSize = 1000; // 每次批量写入1000条消息

SimpleProducerConfig producerConfig = new SimpleProducerConfig.Builder()
        .setDataDir(dataDir)
        .setKeepDays(keepDays)
        .setFlushBatchSize(flushBatchSize)
        .build();

SimpleProducer producer = new SimpleProducer(producerConfig);
```


### 发送消息

```java
boolean success = producer.offer("Hello, World!");
if (success) {
    System.out.println("Message sent successfully.");
} else {
    System.out.println("Failed to send message.");
}
```


### 配置消费者

```java
File dataDir = new File("/path/to/data");
File positionFile = new File("/path/to/position/file");
int cacheSize = 10000; // 缓存大小
long flushPositionInterval = 5000; // 每5秒刷新一次位置
String consumerId = "consumer1"; // 消费者ID
int pullInterval = 100; // 每100毫秒拉取一次消息

SimpleConsumerConfig consumerConfig = new SimpleConsumerConfig.Builder()
        .setDataDir(dataDir)
        .setPositionFile(positionFile)
        .setCacheSize(cacheSize)
        .setFlushPositionInterval(flushPositionInterval)
        .setConsumerId(consumerId)
        .setPullInterval(pullInterval)
        .build();

SimpleConsumer consumer = new SimpleConsumer(consumerConfig);
```


### 读取消息

```java
try {
    QueueMessage message = consumer.take();
    if (message != null) {
        System.out.println("Received message: " + message.getMessage());
        consumer.ack(message); // 确认消息已处理
    }
} catch (InterruptedException e) {
    Thread.currentThread().interrupt();
}
```


### 关闭资源

```java
producer.close();
consumer.close();
```


## 依赖项

本项目依赖于以下库：
- [Chronicle Queue](https://github.com/OpenHFT/Chronicle-Queue)：用于实现高性能、持久化的消息队列。
- [SLF4J](http://www.slf4j.org/)：用于日志记录。

请确保在项目的 `pom.xml` 或 `build.gradle` 文件中添加相应的依赖项。

## 常见问题

1. **Q: 如何调整生产者的批量写入大小？**
   - A: 可以通过 `SimpleProducerConfig.Builder.setFlushBatchSize(int)` 方法设置批量写入的大小。

2. **Q: 消费者如何处理未确认的消息？**
   - A: 消费者会将未确认的消息保留在缓存中，直到调用 `ack` 方法确认消息已处理。如果消费者重启，可以从上次确认的位置继续读取。

3. **Q: 如何控制日志文件的保留时间？**
   - A: 可以通过 `SimpleProducerConfig.Builder.setKeepDays(int)` 方法设置日志文件的保留天数。设置为 `-1` 表示不删除旧文件。

## 贡献代码

欢迎贡献代码！如果您有任何改进建议或发现任何问题，请提交 Issue 或 Pull Request。

---

希望这个 README 对您有所帮助。如果有任何疑问或建议，请随时联系我。

---

**作者**: frank  
**邮箱**: wz2cool@example.com  
**GitHub**: https://github.com/wz2cool/local-queue  

---

感谢您的关注和支持！