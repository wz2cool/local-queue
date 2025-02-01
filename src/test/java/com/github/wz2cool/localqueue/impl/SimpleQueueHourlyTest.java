package com.github.wz2cool.localqueue.impl;

import com.github.wz2cool.localqueue.IConsumer;
import com.github.wz2cool.localqueue.model.config.SimpleQueueConfig;
import com.github.wz2cool.localqueue.model.enums.ConsumeFromWhere;
import com.github.wz2cool.localqueue.model.enums.RollCycleType;
import com.github.wz2cool.localqueue.model.message.QueueMessage;
import net.openhft.chronicle.queue.RollCycles;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("all")
public class SimpleQueueHourlyTest {

    private File dir;
    private SimpleQueueConfig config;

    @BeforeEach
    public void setUp() throws IOException {
        dir = new File("./test");
        FileUtils.deleteDirectory(dir);
        config = new SimpleQueueConfig.Builder()
                .setDataDir(dir)
                .setKeepDays(1)
                .setRollCycleType(RollCycleType.HOURLY)
                .build();
    }

    @AfterEach
    public void cleanUp() throws IOException, InterruptedException {
        Thread.sleep(300);
        FileUtils.deleteDirectory(dir);
    }

    @Test
    public void testConsume() throws InterruptedException {
        try (SimpleQueue queue = new SimpleQueue(config)) {
            queue.addCloseListener(() -> {
                System.out.println("queue close");
            });

            for (int i = 0; i < 100; i++) {
                queue.offer("test" + i);
            }
            Thread.sleep(100);
            Thread consumer1Thread = new Thread(() -> {
                try {
                    String consumerId = "consumer1";
                    IConsumer consumer = queue.getConsumer(consumerId, ConsumeFromWhere.FIRST);
                    Thread.sleep(100);
                    List<QueueMessage> queueMessages = consumer.batchTake(10);
                    assertEquals(10, queueMessages.size());
                    assertEquals("test0", queueMessages.get(0).getContent());
                    consumer.ack(queueMessages);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            Thread consumer2Thread = new Thread(() -> {
                try {
                    String consumerId = "consumer2";
                    IConsumer consumer = queue.getConsumer(consumerId, ConsumeFromWhere.FIRST);
                    Thread.sleep(100);
                    List<QueueMessage> queueMessages = consumer.batchTake(20);
                    assertEquals(20, queueMessages.size());
                    assertEquals("test0", queueMessages.get(0).getContent());
                    System.out.println("consumer2:" + queueMessages.get(0).getPosition());

                    long sequenceNumber = RollCycles.FAST_DAILY.toSequenceNumber(queueMessages.get(1).getPosition());
                    System.out.println(sequenceNumber);
                    Instant instant = Instant.ofEpochSecond(queueMessages.get(0).getPosition());
                    System.out.println(instant);
                    consumer.ack(queueMessages);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            consumer1Thread.start();
            consumer2Thread.start();
            consumer1Thread.join();
            consumer2Thread.join();
        }
    }

    @Test
    public void testConsumeLast() throws InterruptedException {
        try (SimpleQueue queue = new SimpleQueue(config)) {
            queue.offer("test1");
            Thread.sleep(100);
            // consumer from last
            IConsumer consumer1 = queue.getConsumer("consumer1");
            Optional<QueueMessage> messageOptional = consumer1.poll();
            assertFalse(messageOptional.isPresent());
            TimeUnit.MILLISECONDS.sleep(100);
            queue.offer("test2");
            TimeUnit.MILLISECONDS.sleep(100);
            messageOptional = consumer1.poll();
            assertTrue(messageOptional.isPresent());
            assertEquals("test2", messageOptional.get().getContent());
        }
    }

    @Test
    public void testConsumeFirst() throws InterruptedException {
        try (SimpleQueue queue = new SimpleQueue(config)) {
            queue.offer("test1");
            queue.offer("test2");
            IConsumer consumer1 = queue.getConsumer("consumer1", ConsumeFromWhere.FIRST);
            TimeUnit.MILLISECONDS.sleep(100);
            Optional<QueueMessage> messageOptional = consumer1.poll();
            assertTrue(messageOptional.isPresent());
            assertEquals("test1", messageOptional.get().getContent());
        }
    }

    @Test
    public void multiCallClose() throws InterruptedException {
        try (SimpleQueue queue = new SimpleQueue(config)) {
            queue.close();
        }
        assertTrue(true, "no error return.");
    }
}
