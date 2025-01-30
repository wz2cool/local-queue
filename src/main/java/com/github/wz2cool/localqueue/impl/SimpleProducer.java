package com.github.wz2cool.localqueue.impl;

import com.github.wz2cool.localqueue.IProducer;
import com.github.wz2cool.localqueue.model.config.SimpleProducerConfig;
import com.github.wz2cool.localqueue.model.message.InternalWriteMessage;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * simple writer
 *
 * @author frank
 */
public class SimpleProducer implements IProducer, AutoCloseable {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final SimpleProducerConfig config;
    private final SingleChronicleQueue queue;
    private final LinkedBlockingQueue<String> messageCache = new LinkedBlockingQueue<>();
    // should only call by flushExecutor
    private final ExcerptAppender mainAppender;
    private final ExecutorService flushExecutor = Executors.newSingleThreadExecutor();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
    private final Lock internalLock = new ReentrantLock();

    private volatile boolean isFlushRunning = true;
    private volatile boolean isClosing = false;
    private volatile boolean isClosed = false;

    public SimpleProducer(final SimpleProducerConfig config) {
        this.config = config;
        this.queue = ChronicleQueue.singleBuilder(config.getDataDir()).rollCycle(RollCycles.FAST_DAILY).build();
        this.mainAppender = initMainAppender();
        flushExecutor.execute(this::flush);
        scheduler.scheduleAtFixedRate(() -> cleanUpOldFiles(config.getKeepDays()), 0, 1, TimeUnit.HOURS);
    }

    private ExcerptAppender initMainAppender() {
        return CompletableFuture.supplyAsync(this.queue::createAppender, this.flushExecutor).join();
    }

    /// region flush to file
    private void flush() {
        while (isFlushRunning && !isClosing) {
            flushMessages(config.getFlushBatchSize());
        }
    }

    private void stopFlush() {
        isFlushRunning = false;
    }

    private final List<String> tempFlushMessages = new ArrayList<>();

    private void flushMessages(int batchSize) {
        try {
            logDebug("[flushInternal] start");
            if (tempFlushMessages.isEmpty()) {
                // take 主要作用就是卡主线程
                String firstItem = this.messageCache.poll(config.getFlushInterval(), TimeUnit.MILLISECONDS);
                if (firstItem == null) {
                    return;
                }
                this.tempFlushMessages.add(firstItem);
                // 如果空了从消息缓存放入待刷消息
                this.messageCache.drainTo(tempFlushMessages, batchSize - 1);
            }
            flushMessages(tempFlushMessages);
            tempFlushMessages.clear();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } finally {
            logDebug("[flushInternal] end");
        }
    }

    private void flushMessages(final List<String> messages) {
        try {
            logDebug("[flushMessages] start");
            internalLock.lock();
            if (!isFlushRunning || isClosing) {
                return;
            }

            for (String message : messages) {
                long writeTime = System.currentTimeMillis();
                InternalWriteMessage internalWriteMessage = new InternalWriteMessage();
                internalWriteMessage.setWriteTime(writeTime);
                internalWriteMessage.setContent(message);
                mainAppender.writeBytes(internalWriteMessage);
            }
        } finally {
            internalLock.unlock();
            logDebug("[flushMessages] end");
        }
    }

    /// endregion


    @Override
    public boolean offer(String message) {
        return this.messageCache.offer(message);
    }

    /**
     * get the last position
     *
     * @return this last position
     */
    public long getLastPosition() {
        return this.queue.lastIndex();
    }

    /// region close

    /**
     * is closed.
     *
     * @return true if the Producer is closed, false otherwise.
     */
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public void close() {
        try {
            logDebug("[close] start");
            isClosing = true;
            internalLock.lock();
            stopFlush();
            internalLock.unlock();
            queue.close();
            flushExecutor.shutdown();
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(1, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
                if (!flushExecutor.awaitTermination(1, TimeUnit.SECONDS)) {
                    flushExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                flushExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            isClosed = true;
        } finally {
            logDebug("[close] end");
        }
    }

    private void cleanUpOldFiles(int keepDays) {
        if (keepDays == -1) {
            // no need clean up old files
            return;
        }
        logDebug("[cleanUpOldFiles] start");
        try {
            // Assuming .cq4 is the file extension for Chronicle Queue
            File[] files = config.getDataDir().listFiles((dir, name) -> name.endsWith(".cq4"));
            if (files == null || files.length == 0) {
                logDebug("[cleanUpOldFiles] no files found");
                return;
            }
            LocalDate now = LocalDate.now();
            LocalDate keepStartDate = now.minusDays(keepDays);
            for (File file : files) {
                cleanUpOldFile(file, keepStartDate);
            }
        } catch (Exception ex) {
            logger.error("[cleanUpOldFiles] error", ex);
        } finally {
            logDebug("[cleanUpOldFiles] end");
        }
    }

    private void cleanUpOldFile(final File file, final LocalDate keepDate) throws IOException {
        String fileName = file.getName();
        String dateString = fileName.substring(0, 8);
        LocalDate localDate = LocalDate.parse(dateString, this.dateFormatter);
        if (localDate.isBefore(keepDate)) {
            Files.deleteIfExists(file.toPath());
            logDebug("[cleanUpOldFile] Deleted old file: {}", file.getName());
        }
    }

    /// endregion

    private void logDebug(String format) {
        if (logger.isDebugEnabled()) {
            logDebug(format);
        }
    }

    private void logDebug(String format, Object arg) {
        if (logger.isDebugEnabled()) {
            logDebug(format, arg);
        }
    }
}
