package com.github.wz2cool.localqueue.impl;

import com.github.wz2cool.localqueue.IReader;
import com.github.wz2cool.localqueue.model.config.SimpleQueueConfig;
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

import static org.junit.jupiter.api.Assertions.assertEquals;

@SuppressWarnings("all")
public class SimpleQueueTest {

    private File dir;
    private SimpleQueueConfig config;

    @BeforeEach
    public void setUp() throws IOException {
        dir = new File("./test");
        FileUtils.deleteDirectory(dir);
        config = new SimpleQueueConfig.Builder()
                .setDataDir(dir)
                .setKeepDays(1)
                .build();
    }

    @AfterEach
    public void cleanUp() throws IOException {
        FileUtils.deleteDirectory(dir);
    }

    @Test
    public void test() throws InterruptedException {
        try (SimpleQueue queue = new SimpleQueue(config)) {
            for (int i = 0; i < 100; i++) {
                queue.offer("test" + i);
            }
            Thread.sleep(100);
            Thread reader1Thread = new Thread(() -> {
                try {
                    String readerKey = "reader1";
                    IReader reader = queue.getReader(readerKey);
                    Thread.sleep(100);
                    List<QueueMessage> queueMessages = reader.batchTake(10);
                    assertEquals(10, queueMessages.size());
                    assertEquals("test0", queueMessages.get(0).getContent());
                    reader.ack(queueMessages);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            Thread reader2Thread = new Thread(() -> {
                try {
                    String readerKey = "reader2";
                    IReader reader = queue.getReader(readerKey);
                    Thread.sleep(100);
                    List<QueueMessage> queueMessages = reader.batchTake(20);
                    assertEquals(20, queueMessages.size());
                    assertEquals("test0", queueMessages.get(0).getContent());
                    System.out.println("reader2:" + queueMessages.get(0).getPosition());

                    long sequenceNumber = RollCycles.FAST_DAILY.toSequenceNumber(queueMessages.get(1).getPosition());
                    System.out.println(sequenceNumber);
                    Instant instant = Instant.ofEpochSecond(queueMessages.get(0).getPosition());
                    System.out.println(instant);
                    reader.ack(queueMessages);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            reader1Thread.start();
            reader2Thread.start();
            reader1Thread.join();
            reader2Thread.join();
        }
    }
}
