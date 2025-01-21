package com.github.wz2cool.localqueue.impl;

import com.github.wz2cool.localqueue.IWriter;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * 写入器
 *
 * @author frank
 */
public class SimpleWriter implements IWriter, AutoCloseable {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private static final int FLUSH_BATCH_SIZE = 1000;
    private final File queueDir;
    private final SingleChronicleQueue queue;
    private final LinkedBlockingQueue<String> messageCache = new LinkedBlockingQueue<>();
    private final ThreadLocal<ExcerptAppender> appenderThreadLocal;
    private final ExecutorService flushExecutor = Executors.newSingleThreadExecutor();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public SimpleWriter(File queueDir, int keepDays) {
        this.queueDir = queueDir;
        this.queue = ChronicleQueue.singleBuilder(queueDir).rollCycle(RollCycles.FAST_DAILY).build();
        this.appenderThreadLocal = ThreadLocal.withInitial(this.queue::createAppender);
        flushExecutor.execute(this::flush);
        scheduler.scheduleAtFixedRate(() -> cleanUpOldFiles(keepDays), 0, 1, TimeUnit.HOURS);
    }


    /// region flush to file

    private volatile boolean isFlushing = true;

    private void stopFlush() {
        this.isFlushing = false;
    }

    private void flush() {
        while (this.isFlushing) {
            flushInternal(FLUSH_BATCH_SIZE);
        }
    }

    private final List<String> tempFlushMessages = new ArrayList<>();

    private void flushInternal(int batchSize) {
        try {
            if (tempFlushMessages.isEmpty()) {
                // take 主要作用就是卡主线程
                String firstItem = this.messageCache.take();
                this.tempFlushMessages.add(firstItem);
                // 如果空了从消息缓存放入待刷消息
                this.messageCache.drainTo(tempFlushMessages, batchSize - 1);
            }

            // 如果100 一刷可以说明刷的慢了
            if (tempFlushMessages.size() > 100) {
                logger.debug("[flushInternal] > 100 batch flush: {}", tempFlushMessages.size());
            }

            ExcerptAppender appender = appenderThreadLocal.get();
            for (String message : tempFlushMessages) {
                appender.writeText(message);
            }
            tempFlushMessages.clear();
        } catch (Exception ex) {
            logger.error("[flushInternal] error", ex);
        }
    }

    /// endregion


    @Override
    public boolean write(String message) {
        return this.messageCache.offer(message);
    }

    /// region close

    private volatile boolean isClosed = false;

    /**
     * 是否已经关闭
     *
     * @return true if the writer is closed, false otherwise.
     */
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public void close() {
        stopFlush();
        queue.close();
        flushExecutor.shutdown();
        scheduler.shutdown();
        appenderThreadLocal.remove();
        isClosed = true;
    }

    private void cleanUpOldFiles(int keepDays) {
        logger.debug("[cleanUpOldFiles] start");
        try {
            long currentTime = System.currentTimeMillis();
            // Assuming .cq4 is the file extension for Chronicle Queue
            File[] files = this.queueDir.listFiles((dir, name) -> name.endsWith(".cq4"));
            if (files == null || files.length == 0) {
                return;
            }
            long keepMillis = TimeUnit.DAYS.toMillis(keepDays);
            for (File file : files) {
                long fileCreationTime = file.lastModified();
                if (currentTime - fileCreationTime > keepMillis) {
                    boolean deleted = Files.deleteIfExists(file.toPath());
                    if (deleted) {
                        logger.debug("[cleanUpOldFiles] Deleted old file: {}", file.getName());
                    } else {
                        logger.debug("[cleanUpOldFiles] Failed to delete file: {}", file.getName());
                    }
                }
            }
        } catch (Exception ex) {
            logger.error("[cleanUpOldFiles] error", ex);
        } finally {
            logger.debug("[cleanUpOldFiles] end");
        }
    }

    /// endregion
}
