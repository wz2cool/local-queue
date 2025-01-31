package com.github.wz2cool.localqueue.impl;

import com.github.wz2cool.localqueue.model.config.SimpleConsumerConfig;
import com.github.wz2cool.localqueue.model.config.SimpleProducerConfig;
import com.github.wz2cool.localqueue.model.enums.ConsumeFromWhere;
import com.github.wz2cool.localqueue.model.message.QueueMessage;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("all")
public class SimpleConusmerTest {

    private File dir;
    private SimpleProducerConfig producerConfig;
    private SimpleConsumerConfig consumerConfig;
    private boolean test;


    @BeforeEach
    public void setUp() throws IOException {
        dir = new File("./test");
        FileUtils.deleteDirectory(dir);
        producerConfig = new SimpleProducerConfig.Builder()
                .setDataDir(dir)
                .setKeepDays(1)
                .build();

        consumerConfig = new SimpleConsumerConfig.Builder()
                .setDataDir(dir)
                .setConsumeFromWhere(ConsumeFromWhere.FIRST)
                .setPositionFile(new File("./test/position.txt"))
                .setConsumerId("test")
                .setPullInterval(1)
                .setCacheSize(100)
                .setFlushPositionInterval(10)
                .build();
    }

    @AfterEach
    public void cleanUp() throws IOException, InterruptedException {
        FileUtils.deleteDirectory(dir);
    }

    // region take

    @Test
    public void take_NonEmptyCache_ReturnsQueueMessage() throws InterruptedException {
        try (SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig);
             SimpleProducer simpleProducer = new SimpleProducer(producerConfig)) {
            simpleProducer.offer("test");
            QueueMessage message = simpleConsumer.take();
            assertEquals("test", message.getContent());
        }
    }

    @Test
    public void take_EmptyCache_BlocksUntilMessageAvailable() throws InterruptedException {
        try (SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig);
             SimpleProducer simpleProducer = new SimpleProducer(producerConfig)) {
            Thread consumeThread = new Thread(() -> {
                try {
                    QueueMessage message = simpleConsumer.take();
                    assertEquals("test", message.getContent());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            consumeThread.start();
            simpleProducer.offer("test");
            Thread.sleep(100); // blocking until message available
            consumeThread.join();
        }
    }

    @Test
    public void take_Interrupted_ThrowsInterruptedException() throws InterruptedException {
        try (SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig)) {
            Thread consumeThread = new Thread(() -> {
                try {
                    simpleConsumer.take();
                } catch (InterruptedException e) {
                    assertThrowsExactly(InterruptedException.class, () -> {
                    });
                    Thread.currentThread().interrupt();
                }
            });
            consumeThread.start();
            consumeThread.interrupt();
            assertTrue(consumeThread.isInterrupted());
            consumeThread.join();
        }
    }

    // endregion

    // region batch take

    @Test
    public void batchTake_EmptyCache_BlocksUntilMessageAvailable() throws InterruptedException {
        try (SimpleProducer simpleProducer = new SimpleProducer(producerConfig);
             SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig)) {
            Thread consumeThread = new Thread(() -> {
                try {
                    List<QueueMessage> messages = simpleConsumer.batchTake(1);
                    assertEquals(1, messages.size());
                    assertEquals("test", messages.get(0).getContent());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            consumeThread.start();
            Thread.sleep(100); // 阻塞直到消息可用
            simpleProducer.offer("test");
            consumeThread.join();
        }
    }

    @Test
    public void batchTake_SingleMessage_ReturnsSingleMessage() throws InterruptedException {
        try (SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig);
             SimpleProducer simpleProducer = new SimpleProducer(producerConfig)) {
            simpleProducer.offer("test");
            // make sure the message is available
            Thread.sleep(100);
            List<QueueMessage> messages = simpleConsumer.batchTake(10);
            assertEquals(1, messages.size());
            assertEquals("test", messages.get(0).getContent());
        }
    }

    @Test
    public void batchTake_MultipleMessages_ReturnsMultipleMessages() throws InterruptedException {
        try (SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig);
             SimpleProducer simpleProducer = new SimpleProducer(producerConfig)) {
            simpleProducer.offer("test1");
            simpleProducer.offer("test2");
            simpleProducer.offer("test3");
            Thread.sleep(100);
            List<QueueMessage> messages = simpleConsumer.batchTake(10);
            assertEquals(3, messages.size());
            assertEquals("test1", messages.get(0).getContent());
            assertEquals("test2", messages.get(1).getContent());
            assertEquals("test3", messages.get(2).getContent());
        }
    }

    @Test
    public void batchTake_MaxBatchSize_LimitedMessages() throws InterruptedException {
        try (SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig);
             SimpleProducer simpleProducer = new SimpleProducer(producerConfig)) {
            simpleProducer.offer("test1");
            simpleProducer.offer("test2");
            simpleProducer.offer("test3");
            Thread.sleep(100);
            List<QueueMessage> messages = simpleConsumer.batchTake(2);
            assertEquals(2, messages.size());
            assertEquals("test1", messages.get(0).getContent());
            assertEquals("test2", messages.get(1).getContent());
        }
    }
    // endregion

    // region take with timeout
    @Test
    public void take_MessageAvailable_ReturnsQueueMessage() throws InterruptedException {
        try (SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig);
             SimpleProducer simpleProducer = new SimpleProducer(producerConfig)) {
            simpleProducer.offer("test");
            Optional<QueueMessage> message = simpleConsumer.take(100, TimeUnit.MILLISECONDS);
            assertTrue(message.isPresent());
            assertEquals("test", message.get().getContent());
        }
    }

    @Test
    public void take_MessageNotAvailable_ReturnsEmptyOptional() throws InterruptedException {
        try (SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig);
             SimpleProducer simpleProducer = new SimpleProducer(producerConfig)) {
            Optional<QueueMessage> message = simpleConsumer.take(100, TimeUnit.MILLISECONDS);
            assertFalse(message.isPresent());
        }
    }

    @Test
    public void take_TimeoutExpires_ReturnsEmptyOptional() throws InterruptedException {
        try (SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig);
             SimpleProducer simpleProducer = new SimpleProducer(producerConfig)) {
            Thread writeThread = new Thread(() -> {
                try {
                    Thread.sleep(200);
                    simpleProducer.offer("test");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            writeThread.start();
            Optional<QueueMessage> message = simpleConsumer.take(100, TimeUnit.MILLISECONDS);
            writeThread.join();
            assertFalse(message.isPresent());
        }
    }

    @Test
    public void take_WithinTimeout_ReturnsValue() throws InterruptedException {
        try (SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig);
             SimpleProducer simpleProducer = new SimpleProducer(producerConfig)) {
            Thread writeThread = new Thread(() -> {
                try {
                    Thread.sleep(10);
                    simpleProducer.offer("test");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            writeThread.start();
            Optional<QueueMessage> message = simpleConsumer.take(100, TimeUnit.MILLISECONDS);
            writeThread.join();
            assertTrue(message.isPresent());
        }
    }

    // endregion

    // region batch take with timeout

    @Test
    public void batchTake_MessageAvailable_ReturnsQueueMessage() throws InterruptedException {
        try (SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig);
             SimpleProducer simpleProducer = new SimpleProducer(producerConfig)) {
            simpleProducer.offer("test");
            simpleProducer.offer("test2");
            Thread.sleep(100);
            List<QueueMessage> queueMessages = simpleConsumer.batchTake(10, 100, TimeUnit.MILLISECONDS);
            assertEquals(2, queueMessages.size());
            assertEquals("test", queueMessages.get(0).getContent());
            assertEquals("test2", queueMessages.get(1).getContent());
        }
    }

    @Test
    public void batchTake_MessageNotAvailable_ReturnsEmptyOptional() throws InterruptedException {
        try (SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig);
             SimpleProducer simpleProducer = new SimpleProducer(producerConfig)) {
            List<QueueMessage> queueMessages = simpleConsumer.batchTake(10, 100, TimeUnit.MILLISECONDS);
            assertEquals(0, queueMessages.size());
        }
    }

    @Test
    public void batchTake_TimeoutExpires_ReturnsEmptyOptional() throws InterruptedException {
        try (SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig);
             SimpleProducer simpleProducer = new SimpleProducer(producerConfig)) {
            Thread writeThread = new Thread(() -> {
                try {
                    Thread.sleep(200);
                    simpleProducer.offer("test1");
                    simpleProducer.offer("test2");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            writeThread.start();
            List<QueueMessage> queueMessages = simpleConsumer.batchTake(10, 100, TimeUnit.MILLISECONDS);
            writeThread.join();
            assertEquals(0, queueMessages.size());
        }
    }

    @Test
    public void batchTake_WithinTimeout_ReturnsValue() throws InterruptedException {
        try (SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig);
             SimpleProducer simpleProducer = new SimpleProducer(producerConfig)) {
            Thread writeThread = new Thread(() -> {
                try {
                    Thread.sleep(10);
                    simpleProducer.offer("test1");
                    simpleProducer.offer("test2");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            writeThread.start();
            Thread.sleep(100);
            List<QueueMessage> queueMessages = simpleConsumer.batchTake(10, 100, TimeUnit.MILLISECONDS);
            writeThread.join();
            assertEquals(2, queueMessages.size());
            assertEquals("test1", queueMessages.get(0).getContent());
            assertEquals("test2", queueMessages.get(1).getContent());
        }
    }

    // endregion

    // region poll

    @Test
    public void poll_EmptyCache_ReturnsEmptyOptional() {
        try (SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig)) {
            Optional<QueueMessage> read = simpleConsumer.poll();
            assertFalse(read.isPresent());
        }
    }

    @Test
    public void poll_NonEmptyCache_ReturnsQueueMessage() throws InterruptedException {
        try (SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig);
             SimpleProducer simpleProducer = new SimpleProducer(producerConfig)) {
            simpleProducer.offer("test");
            // make sure the cache is filled
            Thread.sleep(100);
            Optional<QueueMessage> read = simpleConsumer.poll();
            assertTrue(read.isPresent());
            assertEquals("test", read.get().getContent());
        }
    }

    // endregion

    // region batch poll

    @Test
    public void batchPoll_EmptyCache_ReturnsEmptyOptional() {
        try (SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig)) {
            List<QueueMessage> queueMessages = simpleConsumer.batchPoll(10);
            assertEquals(0, queueMessages.size());
        }
    }

    @Test
    public void batchPoll_NonEmptyCache_ReturnsQueueMessage() throws InterruptedException {
        try (SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig);
             SimpleProducer simpleProducer = new SimpleProducer(producerConfig)) {
            simpleProducer.offer("test");
            simpleProducer.offer("test2");
            // make sure the cache is filled
            Thread.sleep(100);
            List<QueueMessage> queueMessages = simpleConsumer.batchPoll(10);
            assertEquals(2, queueMessages.size());
            assertEquals("test", queueMessages.get(0).getContent());
            assertEquals("test2", queueMessages.get(1).getContent());
        }
    }

    // endregion

    // region ack


    @Test
    public void ack_NullMessages_NoChange() {
        try (SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig)) {
            simpleConsumer.ack((QueueMessage) null);
            assertEquals(-1, simpleConsumer.getAckedReadPosition());
        }
    }

    @Test
    public void ack_EmptyMessages_NoChange() {
        try (SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig)) {
            simpleConsumer.ack(Collections.emptyList());
            assertEquals(-1, simpleConsumer.getAckedReadPosition());
        }
    }

    @Test
    public void ack_NonEmptyMessages_PositionUpdated() {
        try (SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig)) {
            List<QueueMessage> messages = new ArrayList<>();
            messages.add(new QueueMessage(UUID.randomUUID().toString(), 0, 1L, "message1", System.currentTimeMillis()));
            messages.add(new QueueMessage(UUID.randomUUID().toString(), 0, 2L, "message2", System.currentTimeMillis()));
            messages.add(new QueueMessage(UUID.randomUUID().toString(), 0, 3L, "message3", System.currentTimeMillis()));
            simpleConsumer.ack(messages);
            assertEquals(3L, simpleConsumer.getAckedReadPosition());
        }
    }

    // endreigon

    // region close

    @Test
    public void close_CloseConsumer_ConsumerClosed() {
        SimpleConsumer test;
        try (SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig)) {
            test = simpleConsumer;

        }
        assertTrue(test.isClosed());
    }

    // endregion

    // region read position

    @Test
    public void resumePosition() throws InterruptedException {
        try (SimpleProducer simpleProducer = new SimpleProducer(producerConfig)) {
            for (int i = 0; i < 100; i++) {
                String message = "msg" + i;
                simpleProducer.offer(message);
            }
            // make sure data has been flushed.
            Thread.sleep(100);
            try (SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig)) {
                // make sure data has been read to cache.
                Thread.sleep(100);
                List<QueueMessage> queueMessages = simpleConsumer.batchTake(50);
                assertEquals(50, queueMessages.size());
                simpleConsumer.ack(queueMessages);
                // make surce position has been flushed.
                Thread.sleep(500);
            }

            try (SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig)) {
                Thread.sleep(100);
                List<QueueMessage> queueMessages = simpleConsumer.batchTake(50);
                assertEquals(50, queueMessages.size());
                assertEquals("msg50", queueMessages.get(0).getContent());
                simpleConsumer.ack(queueMessages);
            }
        }
    }

    // endregion

    // region moveToPosition

    @Test
    public void moveToPosition_valid_position() throws Exception {
        // 写入一条消息到队列中
        try (SimpleProducer producer = new SimpleProducer(producerConfig)) {
            producer.offer("test1");
            producer.offer("test2");
            producer.offer("test3");
            Thread.sleep(100);
            long messagePosition;
            try (SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig)) {
                QueueMessage message = simpleConsumer.take();
                simpleConsumer.ack(message);
                assertEquals("test1", message.getContent());
                // make surce position has been flushed.
                messagePosition = message.getPosition();
                Thread.sleep(100);

            }
            try (SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig)) {
                QueueMessage message2 = simpleConsumer.take();
                simpleConsumer.ack(message2);
                assertEquals("test2", message2.getContent());
                // make surce position has been flushed.
                Thread.sleep(100);
                // *** 指向test1 的位置
                boolean moveToResult = simpleConsumer.moveToPosition(messagePosition);
                assertTrue(moveToResult);
                Thread.sleep(100);
                QueueMessage message = simpleConsumer.take();
                assertEquals("test1", message.getContent());
                simpleConsumer.ack(message);
                // make surce position has been flushed.
                Thread.sleep(100);
            }
            // 在继续读后面 还是test2
            try (SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig)) {
                QueueMessage message2 = simpleConsumer.take();
                simpleConsumer.ack(message2);
                assertEquals("test2", message2.getContent());
                // make surce position has been flushed.
                Thread.sleep(100);
            }
        }

    }

    @Test
    public void moveToPosition_invalid_position() throws Exception {
        // 写入一条消息到队列中
        try (SimpleProducer producer = new SimpleProducer(producerConfig)) {
            producer.offer("test1");
            producer.offer("test2");
            producer.offer("test3");
            Thread.sleep(100);
            long messagePosition;
            try (SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig)) {
                QueueMessage message = simpleConsumer.take();
                simpleConsumer.ack(message);
                assertEquals("test1", message.getContent());
                // make surce position has been flushed.
                messagePosition = message.getPosition();
                Thread.sleep(100);

            }
            try (SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig)) {
                QueueMessage message2 = simpleConsumer.take();
                simpleConsumer.ack(message2);
                assertEquals("test2", message2.getContent());
                // make surce position has been flushed.
                Thread.sleep(100);
                // *** 指向test1 的位置
                boolean moveToResult = simpleConsumer.moveToPosition(9999999999999L);
                assertFalse(moveToResult);
                Thread.sleep(100);
                QueueMessage message = simpleConsumer.take();
                // 重置位置失败，接着test2 向下读
                assertEquals("test3", message.getContent());
                simpleConsumer.ack(message);
                // make surce position has been flushed.
                Thread.sleep(100);
            }
        }

    }


    // endregion

    // region moveToTimestamp
    @Test
    public void moveToTimestamp_valid_timestamp() throws Exception {
        try (SimpleProducer producer = new SimpleProducer(producerConfig);
             SimpleConsumer consumer = new SimpleConsumer(consumerConfig)) {
            List<Long> timeList = new ArrayList<>();
            for (int i = 0; i < 300; i++) {
                String msg = "msg" + i;
                producer.offer(msg);
                long msg1Time = System.currentTimeMillis();
                Thread.sleep(10);
                timeList.add(msg1Time);
            }
            Thread.sleep(100);
            int testIndex = 22;
            Long time = timeList.get(testIndex);
            long now = System.currentTimeMillis();
            boolean moveToResult = consumer.moveToTimestamp(time);
            long end = System.currentTimeMillis();
            System.out.println("moveToTimestamp cost:" + (end - now));
            assertTrue(moveToResult);
            // make sure apply success.
            Thread.sleep(100);
            QueueMessage message = consumer.take();
            assertEquals("msg" + testIndex, message.getContent());
        }
    }

    @Test
    public void moveToTimestamp_invalid_timestamp() throws Exception {
        try (SimpleProducer producer = new SimpleProducer(producerConfig);
             SimpleConsumer consumer = new SimpleConsumer(consumerConfig)) {
            List<Long> timeList = new ArrayList<>();
            for (int i = 0; i < 300; i++) {
                String msg = "msg" + i;
                producer.offer(msg);
                long msg1Time = System.currentTimeMillis();
                Thread.sleep(10);
                timeList.add(msg1Time);
            }

            long now = System.currentTimeMillis();
            boolean moveToResult = consumer.moveToTimestamp(now);
            long end = System.currentTimeMillis();
            System.out.println("moveToTimestamp cost:" + (end - now));
            assertFalse(moveToResult);
        }
    }

    // endregion

    // region get

    @Test
    public void get_NullMessageKey_ReturnsEmptyOptional() throws InterruptedException {
        try (SimpleProducer producer = new SimpleProducer(producerConfig);
             SimpleConsumer consumer = new SimpleConsumer(consumerConfig)) {
            List<Long> timeList = new ArrayList<>();
            for (int i = 0; i < 300; i++) {
                String key = "key" + i;
                String msg = "msg" + i;
                producer.offer(key, msg);
                long msg1Time = System.currentTimeMillis();
                Thread.sleep(10);
                timeList.add(msg1Time);
            }

            Optional<QueueMessage> result = consumer.get(null);
            assertFalse(result.isPresent());
        }
    }

    @Test
    public void get_EmptyMessageKey_ReturnsEmptyOptional() throws InterruptedException {
        try (SimpleProducer producer = new SimpleProducer(producerConfig);
             SimpleConsumer consumer = new SimpleConsumer(consumerConfig)) {
            List<Long> timeList = new ArrayList<>();
            for (int i = 0; i < 300; i++) {
                String key = "key" + i;
                String msg = "msg" + i;
                producer.offer(key, msg);
                long msg1Time = System.currentTimeMillis();
                Thread.sleep(10);
                timeList.add(msg1Time);
            }

            Optional<QueueMessage> result = consumer.get("", new Timestamp(0).getTime(), System.currentTimeMillis());
            assertFalse(result.isPresent());
        }
    }

    @Test
    public void get_NoMessagesInQueue_ReturnsEmptyOptional() throws InterruptedException {
        try (SimpleProducer producer = new SimpleProducer(producerConfig);
             SimpleConsumer consumer = new SimpleConsumer(consumerConfig)) {
            Optional<QueueMessage> result = consumer.get("nonKey");
            assertFalse(result.isPresent());
        }
    }

    @Test
    public void get_MatchingMessageKey_ReturnsQueueMessage() throws Exception {
        try (SimpleProducer producer = new SimpleProducer(producerConfig);
             SimpleConsumer consumer = new SimpleConsumer(consumerConfig)) {
            List<Long> timeList = new ArrayList<>();
            for (int i = 0; i < 10000; i++) {
                String key = "key" + i;
                String msg = "msg" + i;
                producer.offer(key, msg);
                long msg1Time = System.currentTimeMillis();
                timeList.add(msg1Time);
            }
            Thread.sleep(100);
            long start = System.currentTimeMillis();
            Optional<QueueMessage> result = consumer.get("key9998");
            assertTrue(result.isPresent());
            assertEquals("key9998", result.get().getMessageKey());
            assertEquals("msg9998", result.get().getContent());
            long end = System.currentTimeMillis();
            System.out.println("[get_MatchingMessageKey_ReturnsQueueMessage] get cost:" + (end - start));
        }
    }

    // endregion

    // region get with timestamp
    @Test
    public void get_NullOrEmptyMessageKey_ReturnsEmptyOptional() {
        try (SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig)) {
            Optional<QueueMessage> result = simpleConsumer.get(null,
                    System.currentTimeMillis() - 1000,
                    System.currentTimeMillis() + 1000);
            assertFalse(result.isPresent());

            result = simpleConsumer.get("",
                    System.currentTimeMillis() - 1000,
                    System.currentTimeMillis() + 1000);
            assertFalse(result.isPresent());
        }
    }

    @Test
    public void get_MessageNotInTimeRange_ReturnsEmptyOptional() throws InterruptedException {
        try (SimpleProducer simpleProducer = new SimpleProducer(producerConfig);
             SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig)) {
            simpleProducer.offer("testKey", "content1");
            Thread.sleep(100);

            simpleProducer.offer("testKey", "content2");
            Thread.sleep(100);

            Optional<QueueMessage> result = simpleConsumer.get("testKey",
                    System.currentTimeMillis(),
                    System.currentTimeMillis() + 1000);
            assertFalse(result.isPresent());
        }
    }

    @Test
    public void get_MessageInTimeRange_Returns() throws InterruptedException {
        try (SimpleProducer simpleProducer = new SimpleProducer(producerConfig);
             SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig)) {
            simpleProducer.offer("testKey", "content1");
            Thread.sleep(100);
            long recordTime = System.currentTimeMillis();
            simpleProducer.offer("testKey", "content2");
            Thread.sleep(100);

            Optional<QueueMessage> result = simpleConsumer.get("testKey",
                    recordTime,
                    System.currentTimeMillis());
            assertTrue(result.isPresent());
            assertEquals("content2", result.get().getContent());
        }
    }


    // endregion
}
