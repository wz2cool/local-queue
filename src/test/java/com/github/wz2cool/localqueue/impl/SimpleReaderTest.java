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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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
                .setFlushPositionInterval(100)
                .build();
    }

    @AfterEach
    public void cleanUp() throws IOException, InterruptedException {
        FileUtils.deleteDirectory(dir);
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

    /// region take with timeout
    @Test
    public void take_MessageAvailable_ReturnsQueueMessage() throws InterruptedException {
        try (SimpleReader simpleReader = new SimpleReader(readerConfig);
             SimpleWriter simpleWriter = new SimpleWriter(writerConfig)) {
            simpleWriter.offer("test");
            Optional<QueueMessage> message = simpleReader.take(100, TimeUnit.MILLISECONDS);
            assertTrue(message.isPresent());
            assertEquals("test", message.get().getContent());
        }
    }

    @Test
    public void take_MessageNotAvailable_ReturnsEmptyOptional() throws InterruptedException {
        try (SimpleReader simpleReader = new SimpleReader(readerConfig);
             SimpleWriter simpleWriter = new SimpleWriter(writerConfig)) {
            Optional<QueueMessage> message = simpleReader.take(100, TimeUnit.MILLISECONDS);
            assertFalse(message.isPresent());
        }
    }

    @Test
    public void take_TimeoutExpires_ReturnsEmptyOptional() throws InterruptedException {
        try (SimpleReader simpleReader = new SimpleReader(readerConfig);
             SimpleWriter simpleWriter = new SimpleWriter(writerConfig)) {
            Thread writeThread = new Thread(() -> {
                try {
                    Thread.sleep(200);
                    simpleWriter.offer("test");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            writeThread.start();
            Optional<QueueMessage> message = simpleReader.take(100, TimeUnit.MILLISECONDS);
            writeThread.join();
            assertFalse(message.isPresent());
        }
    }

    @Test
    public void take_WithinTimeout_ReturnsValue() throws InterruptedException {
        try (SimpleReader simpleReader = new SimpleReader(readerConfig);
             SimpleWriter simpleWriter = new SimpleWriter(writerConfig)) {
            Thread writeThread = new Thread(() -> {
                try {
                    Thread.sleep(10);
                    simpleWriter.offer("test");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            writeThread.start();
            Optional<QueueMessage> message = simpleReader.take(100, TimeUnit.MILLISECONDS);
            writeThread.join();
            assertTrue(message.isPresent());
        }
    }

    /// endregion

    /// region batch take with timeout

    @Test
    public void batchTake_MessageAvailable_ReturnsQueueMessage() throws InterruptedException {
        try (SimpleReader simpleReader = new SimpleReader(readerConfig);
             SimpleWriter simpleWriter = new SimpleWriter(writerConfig)) {
            simpleWriter.offer("test");
            simpleWriter.offer("test2");
            Thread.sleep(100);
            List<QueueMessage> queueMessages = simpleReader.batchTake(10, 100, TimeUnit.MILLISECONDS);
            assertEquals(2, queueMessages.size());
            assertEquals("test", queueMessages.get(0).getContent());
            assertEquals("test2", queueMessages.get(1).getContent());
        }
    }

    @Test
    public void batchTake_MessageNotAvailable_ReturnsEmptyOptional() throws InterruptedException {
        try (SimpleReader simpleReader = new SimpleReader(readerConfig);
             SimpleWriter simpleWriter = new SimpleWriter(writerConfig)) {
            List<QueueMessage> queueMessages = simpleReader.batchTake(10, 100, TimeUnit.MILLISECONDS);
            assertEquals(0, queueMessages.size());
        }
    }

    @Test
    public void batchTake_TimeoutExpires_ReturnsEmptyOptional() throws InterruptedException {
        try (SimpleReader simpleReader = new SimpleReader(readerConfig);
             SimpleWriter simpleWriter = new SimpleWriter(writerConfig)) {
            Thread writeThread = new Thread(() -> {
                try {
                    Thread.sleep(200);
                    simpleWriter.offer("test1");
                    simpleWriter.offer("test2");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            writeThread.start();
            List<QueueMessage> queueMessages = simpleReader.batchTake(10, 100, TimeUnit.MILLISECONDS);
            writeThread.join();
            assertEquals(0, queueMessages.size());
        }
    }

    @Test
    public void batchTake_WithinTimeout_ReturnsValue() throws InterruptedException {
        try (SimpleReader simpleReader = new SimpleReader(readerConfig);
             SimpleWriter simpleWriter = new SimpleWriter(writerConfig)) {
            Thread writeThread = new Thread(() -> {
                try {
                    Thread.sleep(10);
                    simpleWriter.offer("test1");
                    simpleWriter.offer("test2");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            writeThread.start();
            Thread.sleep(100);
            List<QueueMessage> queueMessages = simpleReader.batchTake(10, 100, TimeUnit.MILLISECONDS);
            writeThread.join();
            assertEquals(2, queueMessages.size());
            assertEquals("test1", queueMessages.get(0).getContent());
            assertEquals("test2", queueMessages.get(1).getContent());
        }
    }

    /// endregion

    /// region poll

    @Test
    public void poll_EmptyCache_ReturnsEmptyOptional() {
        try (SimpleReader simpleReader = new SimpleReader(readerConfig)) {
            Optional<QueueMessage> read = simpleReader.poll();
            assertFalse(read.isPresent());
        }
    }

    @Test
    public void poll_NonEmptyCache_ReturnsQueueMessage() throws InterruptedException {
        try (SimpleReader simpleReader = new SimpleReader(readerConfig);
             SimpleWriter simpleWriter = new SimpleWriter(writerConfig)) {
            simpleWriter.offer("test");
            // make sure the cache is filled
            Thread.sleep(100);
            Optional<QueueMessage> read = simpleReader.poll();
            assertTrue(read.isPresent());
            assertEquals("test", read.get().getContent());
        }
    }

    /// endregion

    /// region batch poll

    @Test
    public void batchPoll_EmptyCache_ReturnsEmptyOptional() {
        try (SimpleReader simpleReader = new SimpleReader(readerConfig)) {
            List<QueueMessage> queueMessages = simpleReader.batchPoll(10);
            assertEquals(0, queueMessages.size());
        }
    }

    @Test
    public void batchPoll_NonEmptyCache_ReturnsQueueMessage() throws InterruptedException {
        try (SimpleReader simpleReader = new SimpleReader(readerConfig);
             SimpleWriter simpleWriter = new SimpleWriter(writerConfig)) {
            simpleWriter.offer("test");
            simpleWriter.offer("test2");
            // make sure the cache is filled
            Thread.sleep(100);
            List<QueueMessage> queueMessages = simpleReader.batchPoll(10);
            assertEquals(2, queueMessages.size());
            assertEquals("test", queueMessages.get(0).getContent());
            assertEquals("test2", queueMessages.get(1).getContent());
        }
    }

    /// endregion

    /// region ack

    @Test
    public void ack_UpdatePosition_PositionUpdated() throws InterruptedException {
        try (SimpleReader simpleReader = new SimpleReader(readerConfig)) {
            long position = 100L;
            simpleReader.ack(position);
            assertEquals(position, simpleReader.getAckedReadPosition());
        }
    }

    @Test
    public void ack_ConcurrentAccess_ThreadSafety() throws InterruptedException {
        try (SimpleReader simpleReader = new SimpleReader(readerConfig)) {
            int numThreads = 10;
            CountDownLatch latch = new CountDownLatch(numThreads);
            AtomicLong lastPosition = new AtomicLong(0);

            for (int i = 0; i < numThreads; i++) {
                long position = i;
                new Thread(() -> {
                    simpleReader.ack(position);
                    lastPosition.set(position);
                    latch.countDown();
                }).start();
            }

            latch.await();
            assertEquals(lastPosition.get(), simpleReader.getAckedReadPosition());
        }
    }

    @Test
    public void ack_NullMessages_NoChange() {
        try (SimpleReader simpleReader = new SimpleReader(readerConfig)) {
            simpleReader.ack(null);
            assertEquals(-1, simpleReader.getAckedReadPosition());
        }
    }

    @Test
    public void ack_EmptyMessages_NoChange() {
        try (SimpleReader simpleReader = new SimpleReader(readerConfig)) {
            simpleReader.ack(Collections.emptyList());
            assertEquals(-1, simpleReader.getAckedReadPosition());
        }
    }

    @Test
    public void ack_NonEmptyMessages_PositionUpdated() {
        try (SimpleReader simpleReader = new SimpleReader(readerConfig)) {
            List<QueueMessage> messages = new ArrayList<>();
            messages.add(new QueueMessage(1L, "message1"));
            messages.add(new QueueMessage(2L, "message2"));
            messages.add(new QueueMessage(3L, "message3"));
            simpleReader.ack(messages);
            assertEquals(3L, simpleReader.getAckedReadPosition());
        }
    }

    /// endreigon

    /// region close

    @Test
    public void close_CloseReader_ReaderClosed() {
        SimpleReader test;
        try (SimpleReader simpleReader = new SimpleReader(readerConfig)) {
            test = simpleReader;

        }
        assertTrue(test.isClosed());
    }

    /// endregion

    /// region read position

    @Test
    public void resumePosition() throws InterruptedException {
        try (SimpleWriter simpleWriter = new SimpleWriter(writerConfig)) {
            for (int i = 0; i < 100; i++) {
                String message = "msg" + i;
                simpleWriter.offer(message);
            }
            // make sure data has been flushed.
            Thread.sleep(100);
            try (SimpleReader simpleReader = new SimpleReader(readerConfig)) {
                // make sure data has been read to cache.
                Thread.sleep(100);
                List<QueueMessage> queueMessages = simpleReader.batchTake(50);
                assertEquals(50, queueMessages.size());
                simpleReader.ack(queueMessages);
                // make surce position has been flushed.
                Thread.sleep(500);
            }

            try (SimpleReader simpleReader = new SimpleReader(readerConfig)) {
                Thread.sleep(100);
                List<QueueMessage> queueMessages = simpleReader.batchTake(50);
                assertEquals(50, queueMessages.size());
                assertEquals("msg50", queueMessages.get(0).getContent());
                simpleReader.ack(queueMessages);
            }
        }
    }

    /// endregion
}
