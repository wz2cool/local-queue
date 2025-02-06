package com.github.wz2cool.localqueue.impl;

import com.github.wz2cool.localqueue.model.config.SimpleConsumerConfig;
import com.github.wz2cool.localqueue.model.config.SimpleProducerConfig;
import com.github.wz2cool.localqueue.model.enums.ConsumeFromWhere;
import com.github.wz2cool.localqueue.model.message.QueueMessage;
import com.github.wz2cool.localqueue.model.page.PageInfo;
import com.github.wz2cool.localqueue.model.page.SortDirection;
import com.github.wz2cool.localqueue.model.page.UpDown;
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
            simpleConsumer.addCloseListener(() -> {
                System.out.println("consumer close");
            });
            simpleProducer.addCloseListener(() -> {
                System.out.println("producer close");
            });
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
            messages.add(new QueueMessage(null, UUID.randomUUID().toString(), 0, 1L, "message1", System.currentTimeMillis(), null));
            messages.add(new QueueMessage(null, UUID.randomUUID().toString(), 0, 2L, "message2", System.currentTimeMillis(), null));
            messages.add(new QueueMessage(null, UUID.randomUUID().toString(), 0, 3L, "message3", System.currentTimeMillis(), null));
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

    // region findPosition

    @Test
    public void findPosition_EmptyQueue_ReturnsEmptyOptional() {
        try (SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig)) {
            Optional<Long> result = simpleConsumer.findPosition(System.currentTimeMillis());
            assertFalse(result.isPresent());
        }
    }

    @Test
    public void findPosition_InvalidTimestamp_ReturnsEmptyOptional() {
        try (SimpleProducer simpleProducer = new SimpleProducer(producerConfig);
             SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig)) {
            simpleProducer.offer("testKey", "content1");
            simpleProducer.offer("testKey", "content2");
            simpleProducer.offer("testKey", "content3");

            Optional<Long> result = simpleConsumer.findPosition(System.currentTimeMillis());
        }
    }

    @Test
    public void findPosition_ValidTimestamp_Returns() throws InterruptedException {
        try (SimpleProducer simpleProducer = new SimpleProducer(producerConfig);
             SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig)) {

            simpleProducer.offer("testKey1", "content1");
            Thread.sleep(100);
            simpleProducer.offer("testKey2", "content2");
            long expectedTime = System.currentTimeMillis();
            Thread.sleep(100);
            simpleProducer.offer("testKey3", "content3");
            Thread.sleep(100);

            Optional<Long> position = simpleConsumer.findPosition(expectedTime);
            assertTrue(position.isPresent());
            Optional<QueueMessage> queueMessage = simpleConsumer.get(position.get());
            assertTrue(queueMessage.isPresent());
            assertEquals("testKey2", queueMessage.get().getMessageKey());
        }
    }


    // endregion page
    @Test
    public void getPage_NoMessages_EmptyPage() {
        try (SimpleProducer simpleProducer = new SimpleProducer(producerConfig);
             SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig)) {
            PageInfo<QueueMessage> page = simpleConsumer.getPage(SortDirection.ASC, 10);
            assertEquals(0, page.getData().size());
            assertEquals(-1, page.getStart());
            assertEquals(-1, page.getEnd());
        }
    }

    @Test
    public void getPage_WithMessages_FullPage() throws InterruptedException {
        try (SimpleProducer simpleProducer = new SimpleProducer(producerConfig);
             SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig)) {
            for (int i = 0; i < 10; i++) {
                simpleProducer.offer("test" + i);
            }
            Thread.sleep(100);
            PageInfo<QueueMessage> page = simpleConsumer.getPage(SortDirection.ASC, 10);
            assertEquals(10, page.getData().size());
            assertEquals(page.getStart(), page.getData().get(0).getPosition());
            assertEquals(page.getEnd(), page.getData().get(page.getData().size() - 1).getPosition());
        }
    }

    @Test
    public void getPage_WithMessages_PartialPage() throws InterruptedException {
        try (SimpleProducer simpleProducer = new SimpleProducer(producerConfig);
             SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig)) {
            for (int i = 0; i < 100; i++) {
                simpleProducer.offer("test" + i);
            }
            Thread.sleep(100);
            PageInfo<QueueMessage> page = simpleConsumer.getPage(SortDirection.ASC, 10);
            assertEquals(10, page.getData().size());
            assertEquals(page.getStart(), page.getData().get(0).getPosition());
            assertEquals(page.getEnd(), page.getData().get(page.getData().size() - 1).getPosition());
        }
    }

    @Test
    public void getPage_WithMessages_PartialPage_withMoveToPosition() throws InterruptedException {
        try (SimpleProducer simpleProducer = new SimpleProducer(producerConfig);
             SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig)) {
            for (int i = 0; i < 100; i++) {
                simpleProducer.offer("test" + i);
                Thread.sleep(1);
            }
            Thread.sleep(100);
            PageInfo<QueueMessage> page = simpleConsumer.getPage(SortDirection.ASC, 10);
            QueueMessage message = page.getData().get(page.getData().size() - 1);
            long position = message.getPosition();
            // move to position
            PageInfo<QueueMessage> page1 = simpleConsumer.getPage(position, SortDirection.ASC, 10);
            assertEquals(10, page1.getData().size());
            assertEquals(message.getPosition(), page1.getData().get(0).getPosition());
            assertEquals(message.getMessageKey(), page1.getData().get(0).getMessageKey());
        }
    }

    @Test
    public void getPage_WithMessages_PartialPage_withMoveToPosition_DESC() throws InterruptedException {
        try (SimpleProducer simpleProducer = new SimpleProducer(producerConfig);
             SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig)) {
            for (int i = 1; i <= 100; i++) {

                simpleProducer.offer("key" + i, "test" + i);
                Thread.sleep(1);
            }
            Thread.sleep(100);
            PageInfo<QueueMessage> page = simpleConsumer.getPage(SortDirection.DESC, 10);
            QueueMessage lastMessage = page.getData().get(0);
            assertEquals("key100", lastMessage.getMessageKey());

        }
    }

    @Test
    public void getPage_WithMessages_PartialPage_withMoveToPosition_DESC_withUpDown_withPrevPageInfo() throws InterruptedException {
        try (SimpleProducer simpleProducer = new SimpleProducer(producerConfig);
             SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig)) {
            for (int i = 0; i < 100; i++) {
                simpleProducer.offer("key" + i, "content" + i);
            }
            Thread.sleep(100);
            PageInfo<QueueMessage> page = simpleConsumer.getPage(SortDirection.DESC, 10);
            assertEquals(10, page.getData().size());
            assertEquals("key99", page.getData().get(0).getMessageKey());
            PageInfo<QueueMessage> page1 = simpleConsumer.getPage(page, UpDown.DOWN);
            assertEquals(10, page1.getData().size());
            assertEquals("key89", page1.getData().get(0).getMessageKey());

            PageInfo<QueueMessage> page2 = simpleConsumer.getPage(page1, UpDown.UP);
            assertEquals(10, page2.getData().size());
            assertEquals("key99", page2.getData().get(0).getMessageKey());
        }
    }

    @Test
    public void getPage_WithMessages_PartialPage_withMoveToPosition_ASC_withUpDown_withPrevPageInfo() throws InterruptedException {
        try (SimpleProducer simpleProducer = new SimpleProducer(producerConfig);
             SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig)) {
            for (int i = 1; i <= 100; i++) {
                simpleProducer.offer("key" + i, "content" + i);
            }
            Thread.sleep(100);
            PageInfo<QueueMessage> page = simpleConsumer.getPage(SortDirection.ASC, 10);
            assertEquals(10, page.getData().size());
            assertEquals("key1", page.getData().get(0).getMessageKey());
            PageInfo<QueueMessage> page1 = simpleConsumer.getPage(page, UpDown.DOWN);
            assertEquals(10, page1.getData().size());
            assertEquals("key11", page1.getData().get(0).getMessageKey());

            PageInfo<QueueMessage> page2 = simpleConsumer.getPage(page1, UpDown.UP);
            assertEquals(10, page2.getData().size());
            assertEquals("key1", page2.getData().get(0).getMessageKey());
        }
    }


    // endregion

    // region testSelectTag

    @Test
    public void testSelectTag() throws InterruptedException {
        SimpleConsumerConfig consumer1Config = new SimpleConsumerConfig.Builder()
                .setDataDir(dir)
                .setConsumeFromWhere(ConsumeFromWhere.FIRST)
                .setPositionFile(new File("./test/position.txt"))
                .setConsumerId("test")
                .setPullInterval(1)
                .setCacheSize(100)
                .setFlushPositionInterval(10)
                .setSelectorTag("odd")
                .build();

        SimpleConsumerConfig consumer2Config = new SimpleConsumerConfig.Builder()
                .setDataDir(dir)
                .setConsumeFromWhere(ConsumeFromWhere.FIRST)
                .setPositionFile(new File("./test/position.txt"))
                .setConsumerId("test")
                .setPullInterval(1)
                .setCacheSize(100)
                .setFlushPositionInterval(10)
                .setSelectorTag("even")
                .build();

        try (SimpleProducer simpleProducer = new SimpleProducer(producerConfig);
             SimpleConsumer simpleConsumer1 = new SimpleConsumer(consumer1Config);
             SimpleConsumer simpleConsumer2 = new SimpleConsumer(consumer2Config)) {
            for (int i = 1; i <= 100; i++) {
                String tag = i % 2 == 0 ? "even" : "odd";
                simpleProducer.offer(tag, "key" + i, "content" + i);
            }
            Thread.sleep(1000);
            List<QueueMessage> queueMessages = simpleConsumer1.batchPoll(10);
            assertEquals(10, queueMessages.size());
            assertEquals("key1", queueMessages.get(0).getMessageKey());
            for (QueueMessage queueMessage : queueMessages) {
                assertTrue(queueMessage.getTag().equals("odd"));
            }
            simpleConsumer1.ack(queueMessages);

            queueMessages = simpleConsumer2.batchPoll(10);
            assertEquals(10, queueMessages.size());
            assertEquals("key2", queueMessages.get(0).getMessageKey());
            for (QueueMessage queueMessage : queueMessages) {
                assertTrue(queueMessage.getTag().equals("even"));
            }
            simpleConsumer2.ack(queueMessages);
        }
    }

    @Test
    public void testAddHeader() throws InterruptedException {
        try (SimpleProducer simpleProducer = new SimpleProducer(producerConfig);
             SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig)) {
            boolean offerResult = simpleProducer.offer("key1", "", "content1", (headers) -> {
                headers.put("key1", "value1");
                headers.put("key2", "value2");
            });
            assertTrue(offerResult);
            Thread.sleep(100);
            QueueMessage queueMessage = simpleConsumer.take();
            Optional<String> key1Value = queueMessage.getHeaderValue("key1");
            assertTrue(key1Value.isPresent());
            assertEquals("value1", key1Value.get());

            Set<String> headerKeys = queueMessage.getHeaderKeys();
            assertEquals(2, headerKeys.size());
            assertTrue(headerKeys.contains("key1"));
            assertTrue(headerKeys.contains("key2"));
        }

    }

    // endregion

    // region no ack test

    @Test
    public void noAckTest() throws InterruptedException {
        try (SimpleProducer simpleProducer = new SimpleProducer(producerConfig);
             SimpleConsumer simpleConsumer = new SimpleConsumer(consumerConfig)) {
            for (int i = 1; i <= 100; i++) {
                simpleProducer.offer("key" + i, "content" + i);
            }
            Thread.sleep(100);
            List<QueueMessage> queueMessages = simpleConsumer.batchTake(10);
            assertEquals(10, queueMessages.size());
            assertEquals("key1", queueMessages.get(0).getMessageKey());
            // no ack
            Thread.sleep(100);
            queueMessages = simpleConsumer.batchTake(100);
            assertEquals(10, queueMessages.size());
            // still get key1 because previous take no ack
            assertEquals("key1", queueMessages.get(0).getMessageKey());
        }
    }

    // endregion
}
