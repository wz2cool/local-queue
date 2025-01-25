package com.github.wz2cool.localqueue.impl;

import com.github.wz2cool.localqueue.model.config.SimpleQueueConfig;
import com.github.wz2cool.localqueue.model.message.QueueMessage;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
                    List<QueueMessage> queueMessages = queue.batchTake(readerKey, 1);
                    Thread.sleep(100);
                    queueMessages.addAll(queue.batchTake(readerKey, 9));
                    assertEquals(10, queueMessages.size());
                    assertEquals("test0", queueMessages.get(0).getContent());
                    queue.ack(readerKey, queueMessages);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            Thread reader2Thread = new Thread(() -> {
                try {
                    String readerKey = "reader2";
                    List<QueueMessage> queueMessages = queue.batchTake(readerKey, 1);
                    Thread.sleep(100);
                    queueMessages.addAll(queue.batchTake(readerKey, 9));
                    assertEquals(10, queueMessages.size());
                    assertEquals("test0", queueMessages.get(0).getContent());
                    queue.ack(readerKey, queueMessages);
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
