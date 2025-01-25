package com.github.wz2cool.localqueue.impl;

import com.github.wz2cool.localqueue.model.config.SimpleReaderConfig;
import com.github.wz2cool.localqueue.model.config.SimpleWriterConfig;
import com.github.wz2cool.localqueue.model.message.QueueMessage;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("all")
public class SimpleReaderTest {

    private File dir;
    private SimpleWriterConfig writerConfig;
    private SimpleReaderConfig readerConfig;


    @BeforeEach
    public void setUp() throws IOException {
        dir = new File("./test");
        FileUtils.deleteDirectory(dir);
        writerConfig = new SimpleWriterConfig.Builder()
                .setDataDir(dir)
                .setKeepDays(1)
                .build();

        readerConfig = new SimpleReaderConfig.Builder()
                .setDataDir(dir)
                .setPositionFile(new File("./test/position.txt"))
                .setReaderKey("test")
                .setPullInterval(1)
                .setReadCacheSize(100)
                .setFlushPositionInterval(1000)
                .build();
    }

    @AfterEach
    public void cleanUp() throws IOException, InterruptedException {
        FileUtils.deleteDirectory(dir);
    }

    @Test
    public void poll_EmptyCache_ReturnsEmptyOptional() {
        // 队列为空
        try (SimpleReader simpleReader = new SimpleReader(readerConfig);
             SimpleWriter simpleWriter = new SimpleWriter(writerConfig)) {
            // nothing to write, so nothing to raed
            Optional<QueueMessage> read = simpleReader.poll();
            assertFalse(read.isPresent());
        }
    }

    /// region take

    @Test
    public void take_NonEmptyCache_ReturnsQueueMessage() throws InterruptedException {
        try (SimpleReader simpleReader = new SimpleReader(readerConfig);
             SimpleWriter simpleWriter = new SimpleWriter(writerConfig)) {
            simpleWriter.offer("test");
            QueueMessage message = simpleReader.take();
            assertEquals("test", message.getContent());
        }
    }

    @Test
    public void take_EmptyCache_BlocksUntilMessageAvailable() throws InterruptedException {
        try (SimpleReader simpleReader = new SimpleReader(readerConfig);
             SimpleWriter simpleWriter = new SimpleWriter(writerConfig)) {
            Thread readThread = new Thread(() -> {
                try {
                    QueueMessage message = simpleReader.take();
                    assertEquals("test", message.getContent());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            readThread.start();
            Thread.sleep(100); // blocking until message available
            simpleWriter.offer("test");
            readThread.join();
        }
    }

    @Test
    public void take_Interrupted_ThrowsInterruptedException() throws InterruptedException {
        try (SimpleReader simpleReader = new SimpleReader(readerConfig)) {
            Thread readThread = new Thread(() -> {
                try {
                    simpleReader.take();
                } catch (InterruptedException e) {
                    assertThrowsExactly(InterruptedException.class, () -> {
                    });
                    Thread.currentThread().interrupt();
                }
            });
            readThread.start();
            readThread.interrupt();
            assertTrue(readThread.isInterrupted());
            readThread.join();
        }
    }

    /// endregion

    /// region batch take

    @Test
    public void batchTake_EmptyCache_BlocksUntilMessageAvailable() throws InterruptedException {
        try (SimpleWriter simpleWriter = new SimpleWriter(writerConfig);
             SimpleReader simpleReader = new SimpleReader(readerConfig)) {
            Thread readThread = new Thread(() -> {
                try {
                    List<QueueMessage> messages = simpleReader.batchTake(1);
                    assertEquals(1, messages.size());
                    assertEquals("test", messages.get(0).getContent());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            readThread.start();
            Thread.sleep(100); // 阻塞直到消息可用
            simpleWriter.offer("test");
            readThread.join();
        }
    }

    @Test
    public void batchTake_SingleMessage_ReturnsSingleMessage() throws InterruptedException {
        try (SimpleReader simpleReader = new SimpleReader(readerConfig);
             SimpleWriter simpleWriter = new SimpleWriter(writerConfig)) {
            simpleWriter.offer("test");
            // make sure the message is available
            Thread.sleep(100);
            List<QueueMessage> messages = simpleReader.batchTake(10);
            assertEquals(1, messages.size());
            assertEquals("test", messages.get(0).getContent());
        }
    }

    @Test
    public void batchTake_MultipleMessages_ReturnsMultipleMessages() throws InterruptedException {
        try (SimpleReader simpleReader = new SimpleReader(readerConfig);
             SimpleWriter simpleWriter = new SimpleWriter(writerConfig)) {
            simpleWriter.offer("test1");
            simpleWriter.offer("test2");
            simpleWriter.offer("test3");
            Thread.sleep(100);
            List<QueueMessage> messages = simpleReader.batchTake(10);
            assertEquals(3, messages.size());
            assertEquals("test1", messages.get(0).getContent());
            assertEquals("test2", messages.get(1).getContent());
            assertEquals("test3", messages.get(2).getContent());
        }
    }

    @Test
    public void batchTake_MaxBatchSize_LimitedMessages() throws InterruptedException {
        try (SimpleReader simpleReader = new SimpleReader(readerConfig);
             SimpleWriter simpleWriter = new SimpleWriter(writerConfig)) {
            simpleWriter.offer("test1");
            simpleWriter.offer("test2");
            simpleWriter.offer("test3");
            Thread.sleep(100);
            List<QueueMessage> messages = simpleReader.batchTake(2);
            assertEquals(2, messages.size());
            assertEquals("test1", messages.get(0).getContent());
            assertEquals("test2", messages.get(1).getContent());
        }
    }
    /// endregion
}
