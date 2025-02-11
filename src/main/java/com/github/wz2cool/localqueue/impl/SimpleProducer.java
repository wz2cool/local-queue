package com.github.wz2cool.localqueue.impl;

import com.github.wz2cool.localqueue.IProducer;
import com.github.wz2cool.localqueue.event.CloseListener;
import com.github.wz2cool.localqueue.helper.ChronicleQueueHelper;
import com.github.wz2cool.localqueue.model.config.SimpleProducerConfig;
import com.github.wz2cool.localqueue.model.message.internal.HeaderMessage;
import com.github.wz2cool.localqueue.model.message.internal.InternalMessage;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * simple writer
 *
 * @author frank
 */
@SuppressWarnings("Duplicates")
public class SimpleProducer implements IProducer {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final RollCycle defaultRollCycle;
    private final TimeProvider timeProvider;
    private final SimpleProducerConfig config;
    private final SingleChronicleQueue queue;
    private final LinkedBlockingQueue<InternalMessage> messageCache = new LinkedBlockingQueue<>();
    // should only call by flushExecutor
    private final ExcerptAppender mainAppender;
    private final ExecutorService flushExecutor = Executors.newSingleThreadExecutor();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
    private final ConcurrentLinkedQueue<CloseListener> closeListeners = new ConcurrentLinkedQueue<>();

    private final AtomicBoolean isFlushRunning = new AtomicBoolean(true);
    private final AtomicBoolean isClosing = new AtomicBoolean(false);
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final Object closeLocker = new Object();

    public SimpleProducer(final SimpleProducerConfig config) {
        this.config = config;
        this.timeProvider = ChronicleQueueHelper.getTimeProvider(config.getTimeZone());
        this.defaultRollCycle = ChronicleQueueHelper.getRollCycle(config.getRollCycleType());
        this.queue = ChronicleQueue.singleBuilder(config.getDataDir())
                .rollCycle(defaultRollCycle)
                .timeProvider(timeProvider)
                .build();
        this.mainAppender = initMainAppender();
        flushExecutor.execute(this::flush);
        scheduler.scheduleAtFixedRate(() -> cleanUpOldFiles(config.getKeepDays()), 0, 1, TimeUnit.HOURS);
    }

    private ExcerptAppender initMainAppender() {
        return CompletableFuture.supplyAsync(this.queue::createAppender, this.flushExecutor).join();
    }

    // region flush to file

    private void stopFlush() {
        isFlushRunning.set(false);
    }

    private void flush() {
        while (isFlushRunning.get() && !isClosing.get()) {
            flushMessages(config.getFlushBatchSize());
        }
    }

    private final List<InternalMessage> tempFlushMessages = new ArrayList<>();

    private void flushMessages(int batchSize) {
        try {
            logDebug("[flushInternal] start");
            if (tempFlushMessages.isEmpty()) {
                // take 主要作用就是卡主线程
                InternalMessage firstItem = this.messageCache.poll(config.getFlushInterval(), TimeUnit.MILLISECONDS);
                if (firstItem == null) {
                    return;
                }
                this.tempFlushMessages.add(firstItem);
                // 如果空了从消息缓存放入待刷消息
                this.messageCache.drainTo(tempFlushMessages, batchSize - 1);
            }
            doFlushMessages(tempFlushMessages);
            tempFlushMessages.clear();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } catch (Exception ex) {
            logger.error("[flushInternal] flush error", ex);
        } finally {
            logDebug("[flushInternal] end");
        }
    }

    private void doFlushMessages(final List<InternalMessage> messages) {
        synchronized (closeLocker) {
            try {
                logDebug("[flushMessages] start");
                if (isClosing.get()) {
                    logDebug("[flushMessages] producer is closing");
                    return;
                }

                for (InternalMessage message : messages) {
                    long writeTime = System.currentTimeMillis();
                    message.setWriteTime(writeTime);
                    mainAppender.writeBytes(message);
                }
            } finally {
                logDebug("[flushMessages] end");
            }
        }
    }

    // endregion


    @Override
    public boolean offer(String message) {
        return offer(null, message);
    }

    @Override
    public boolean offer(String messageKey, String message) {
        InternalMessage internalMessage = new InternalMessage();
        internalMessage.setContent(message);
        internalMessage.setMessageKey(messageKey);
        return this.messageCache.offer(internalMessage);
    }

    @Override
    public boolean offer(String tag, String messageKey, String message) {
        InternalMessage internalMessage = new InternalMessage();
        internalMessage.setContent(message);
        internalMessage.setMessageKey(messageKey);
        internalMessage.setTag(tag);
        return this.messageCache.offer(internalMessage);
    }


    @Override
    public boolean offer(String tag, String messageKey, String message, Consumer<Map<String, String>> headersConsumer) {
        InternalMessage internalMessage = new InternalMessage();
        internalMessage.setContent(message);
        internalMessage.setMessageKey(messageKey);
        internalMessage.setTag(tag);
        if (Objects.nonNull(headersConsumer)) {
            Map<String, String> headers = new HashMap<>();
            headersConsumer.accept(headers);
            HeaderMessage headerMessage = new HeaderMessage(headers);
            internalMessage.setHeaderMessage(headerMessage);
        }
        return this.messageCache.offer(internalMessage);
    }

    /**
     * get the last position
     *
     * @return this last position
     */
    public long getLastPosition() {
        return this.queue.lastIndex();
    }

    // region close

    /**
     * is closed.
     *
     * @return true if the Producer is closed, false otherwise.
     */
    @Override
    public boolean isClosed() {
        return isClosed.get();
    }

    @Override
    public void close() {
        logDebug("[close] start");
        if (isClosing.get()) {
            logDebug("[close] is closing");
            return;
        }
        isClosing.set(true);
        stopFlush();
        synchronized (closeLocker) {
            try {
                if (!queue.isClosed()) {
                    queue.close();
                }
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
                for (CloseListener closeListener : closeListeners) {
                    closeListener.onClose();
                }
                isClosed.set(true);
            } finally {
                logDebug("[close] end");
            }
        }
    }

    @Override
    public void addCloseListener(CloseListener listener) {
        closeListeners.add(listener);
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

    // endregion

    // region logger
    private void logDebug(String format, String arg) {
        if (logger.isDebugEnabled()) {
            logger.debug(format, arg);
        }
    }


    private void logDebug(String format) {
        if (logger.isDebugEnabled()) {
            logger.debug(format);
        }
    }

    // endregion
}
