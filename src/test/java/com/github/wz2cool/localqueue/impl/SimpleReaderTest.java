package com.github.wz2cool.localqueue.impl;

import com.github.wz2cool.localqueue.model.config.SimpleReaderConfig;
import com.github.wz2cool.localqueue.model.config.SimpleWriterConfig;
import com.github.wz2cool.localqueue.model.message.QueueMessage;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

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

    /// region blockingRead

    @Test
    public void take_NonEmptyCache_ReturnsQueueMessage() throws InterruptedException {
        try (SimpleReader simpleReader = new SimpleReader(readerConfig);
             SimpleWriter simpleWriter = new SimpleWriter(writerConfig)) {
            simpleWriter.write("test");
            QueueMessage message = simpleReader.take();
            assertEquals("test", message.getContent());
        }
    }

    @Test
    public void take_Interrupted_ThrowsInterruptedException() throws InterruptedException {
        try (SimpleReader simpleReader = new SimpleReader(readerConfig)) {
            Thread readThread = new Thread(() -> {
                try {
                    simpleReader.take();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            readThread.start();
            readThread.interrupt();
            assertTrue(readThread.isInterrupted());
        }
    }

    /// endregion

    /// region blockingBatchRead

    @Test
    @Timeout(value = 1, unit = TimeUnit.SECONDS)
    public void batchTake_EmptyCache_ReturnsEmptyList() throws InterruptedException {
        try (SimpleReader simpleReader = new SimpleReader(readerConfig)) {
            List<QueueMessage> messages = simpleReader.batchTake(10);
            assertTrue(messages.isEmpty());
        }
    }


    /// endregion
}
