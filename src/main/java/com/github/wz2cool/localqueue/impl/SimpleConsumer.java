package com.github.wz2cool.localqueue.impl;

import com.github.wz2cool.localqueue.IConsumer;
import com.github.wz2cool.localqueue.event.CloseListener;
import com.github.wz2cool.localqueue.model.config.SimpleConsumerConfig;
import com.github.wz2cool.localqueue.model.enums.ConsumeFromWhere;
import com.github.wz2cool.localqueue.model.message.InternalReadMessage;
import com.github.wz2cool.localqueue.model.message.QueueMessage;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * simple consumer
 *
 * @author frank
 */
public class SimpleConsumer implements IConsumer {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final SimpleConsumerConfig config;
    private final PositionStore positionStore;
    private final SingleChronicleQueue queue;
    // should only call by readCacheExecutor
    private final ExcerptTailer mainTailer;
    private final ExecutorService readCacheExecutor = Executors.newSingleThreadExecutor();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final LinkedBlockingQueue<QueueMessage> messageCache;
    private final ConcurrentLinkedQueue<CloseListener> closeListenerList = new ConcurrentLinkedQueue<>();
    private volatile long ackedReadPosition = -1;
    private volatile boolean isReadToCacheRunning = true;
    private volatile boolean isClosing = false;
    private volatile boolean isClosed = false;
    private final AtomicInteger positionVersion = new AtomicInteger(0);
    private final Lock internalLock = new ReentrantLock();

    /**
     * constructor
     *
     * @param config the config of consumer
     */
    public SimpleConsumer(final SimpleConsumerConfig config) {
        this.config = config;
        this.messageCache = new LinkedBlockingQueue<>(config.getCacheSize());
        this.positionStore = new PositionStore(config.getPositionFile());
        this.queue = ChronicleQueue.singleBuilder(config.getDataDir()).rollCycle(RollCycles.FAST_DAILY).build();
        this.mainTailer = initMainTailer();
        startReadToCache();
        scheduler.scheduleAtFixedRate(this::flushPosition, 0, config.getFlushPositionInterval(), TimeUnit.MILLISECONDS);
    }

    @Override
    public synchronized QueueMessage take() throws InterruptedException {
        return this.messageCache.take();
    }

    @Override
    public synchronized List<QueueMessage> batchTake(int maxBatchSize) throws InterruptedException {
        List<QueueMessage> result = new ArrayList<>(maxBatchSize);
        QueueMessage take = this.messageCache.take();
        result.add(take);
        this.messageCache.drainTo(result, maxBatchSize - 1);
        return result;
    }

    @Override
    public synchronized Optional<QueueMessage> take(long timeout, TimeUnit unit) throws InterruptedException {
        QueueMessage message = this.messageCache.poll(timeout, unit);
        return Optional.ofNullable(message);
    }

    @Override
    public synchronized List<QueueMessage> batchTake(int maxBatchSize, long timeout, TimeUnit unit) throws InterruptedException {
        List<QueueMessage> result = new ArrayList<>(maxBatchSize);
        QueueMessage poll = this.messageCache.poll(timeout, unit);
        if (Objects.nonNull(poll)) {
            result.add(poll);
            this.messageCache.drainTo(result, maxBatchSize - 1);
        }
        return result;
    }

    @Override
    public synchronized Optional<QueueMessage> poll() {
        QueueMessage message = this.messageCache.poll();
        return Optional.ofNullable(message);
    }

    @Override
    public synchronized List<QueueMessage> batchPoll(int maxBatchSize) {
        List<QueueMessage> result = new ArrayList<>(maxBatchSize);
        this.messageCache.drainTo(result, maxBatchSize);
        return result;
    }

    @Override
    public synchronized void ack(final QueueMessage message) {
        if (Objects.isNull(message)) {
            return;
        }

        if (message.getPositionVersion() != positionVersion.get()) {
            return;
        }

        this.ackedReadPosition = message.getPosition();
    }

    @Override
    public synchronized void ack(final List<QueueMessage> messages) {
        if (Objects.isNull(messages) || messages.isEmpty()) {
            return;
        }
        QueueMessage lastOne = messages.get(messages.size() - 1);
        if (lastOne.getPositionVersion() != positionVersion.get()) {
            return;
        }
        this.ackedReadPosition = lastOne.getPosition();
    }

    @Override
    public boolean moveToPosition(final long position) {
        logDebug("[moveToPosition] start");
        stopReadToCache();
        try {
            internalLock.lock();
            return moveToPositionInternal(position);
        } finally {
            internalLock.unlock();
            startReadToCache();
            logDebug("[moveToPosition] end");
        }
    }

    @Override
    public boolean moveToTimestamp(final long timestamp) {
        logDebug("[moveToTimestamp] start, timestamp: {}", timestamp);
        stopReadToCache();
        try {
            internalLock.lock();
            Optional<Long> positionOptional = findPosition(timestamp);
            if (!positionOptional.isPresent()) {
                return false;
            }
            Long position = positionOptional.get();
            return moveToPositionInternal(position);
        } finally {
            internalLock.unlock();
            startReadToCache();
            logDebug("[moveToTimestamp] end");
        }
    }

    @Override
    public Optional<QueueMessage> get(final String messageKey) {
        return get(messageKey, 0L, Long.MAX_VALUE);
    }

    @Override
    public Optional<QueueMessage> get(final String messageKey, long searchTimestampStart, long searchTimestampEnd) {
        if (messageKey == null || messageKey.isEmpty()) {
            return Optional.empty();
        }
        try (ExcerptTailer tailer = queue.createTailer()) {
            while (true) {
                // for performance, ignore read content.
                InternalReadMessage internalReadMessage = new InternalReadMessage(true);
                boolean readResult = tailer.readBytes(internalReadMessage);
                if (!readResult) {
                    return Optional.empty();
                }
                if (internalReadMessage.getWriteTime() < searchTimestampStart) {
                    continue;
                }
                if (internalReadMessage.getWriteTime() > searchTimestampEnd) {
                    return Optional.empty();
                }
                boolean moveToResult = tailer.moveToIndex(tailer.lastReadIndex());
                if (!moveToResult) {
                    return Optional.empty();
                }
                internalReadMessage = new InternalReadMessage();
                readResult = tailer.readBytes(internalReadMessage);
                if (!readResult) {
                    return Optional.empty();
                }
                QueueMessage queueMessage = toQueueMessage(internalReadMessage, tailer.lastReadIndex());
                if (Objects.equals(messageKey, queueMessage.getMessageKey())) {
                    return Optional.of(queueMessage);
                }
            }
        }
    }

    private QueueMessage toQueueMessage(final InternalReadMessage internalReadMessage, final long position) {
        return new QueueMessage(
                internalReadMessage.getMessageKey(),
                positionVersion.get(),
                position,
                internalReadMessage.getContent(),
                internalReadMessage.getWriteTime());
    }

    private boolean moveToPositionInternal(final long position) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                logDebug("[moveToPositionInternal] start, position: {}", position);
                boolean moveToResult = mainTailer.moveToIndex(position);
                if (moveToResult) {
                    positionVersion.incrementAndGet();
                    messageCache.clear();
                    this.ackedReadPosition = position;
                }
                return moveToResult;
            } finally {
                logDebug("[moveToPositionInternal] end");
            }
        }, this.readCacheExecutor).join();
    }

    private Optional<Long> findPosition(final long timestamp) {
        logDebug("[findPosition] start, timestamp: {}", timestamp);
        try (ExcerptTailer excerptTailer = queue.createTailer()) {
            while (true) {
                InternalReadMessage internalReadMessage = new InternalReadMessage(true);
                boolean resultResult = excerptTailer.readBytes(internalReadMessage);
                if (resultResult) {
                    if (internalReadMessage.getWriteTime() >= timestamp) {
                        return Optional.of(excerptTailer.lastReadIndex());
                    }
                } else {
                    return Optional.empty();
                }
            }
        } finally {
            logDebug("[findPosition] end");
        }
    }

    public long getAckedReadPosition() {
        return ackedReadPosition;
    }

    public boolean isClosed() {
        return isClosed;
    }

    private void stopReadToCache() {
        this.isReadToCacheRunning = false;
    }

    private void startReadToCache() {
        this.isReadToCacheRunning = true;
        readCacheExecutor.execute(this::readToCache);
    }

    private void readToCache() {
        try {
            logDebug("[readToCache] start");
            internalLock.lock();
            long pullInterval = config.getPullInterval();
            long fillCacheInterval = config.getFillCacheInterval();
            while (this.isReadToCacheRunning && !isClosing) {
                try {
                    InternalReadMessage internalReadMessage = new InternalReadMessage();
                    boolean readResult = mainTailer.readBytes(internalReadMessage);
                    if (!readResult) {
                        TimeUnit.MILLISECONDS.sleep(pullInterval);
                        continue;
                    }
                    long lastedReadIndex = mainTailer.lastReadIndex();
                    QueueMessage queueMessage = toQueueMessage(internalReadMessage, lastedReadIndex);
                    boolean offerResult = this.messageCache.offer(queueMessage, fillCacheInterval, TimeUnit.MILLISECONDS);
                    if (!offerResult) {
                        // if offer failed, move to last read position
                        mainTailer.moveToIndex(lastedReadIndex);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        } finally {
            internalLock.unlock();
            logDebug("[readToCache] end");
        }
    }

    private ExcerptTailer initMainTailer() {
        return CompletableFuture.supplyAsync(this::initMainTailerInternal, this.readCacheExecutor).join();
    }

    private ExcerptTailer initMainTailerInternal() {
        try {
            logDebug("[initExcerptTailerInternal] start");
            ExcerptTailer tailer = queue.createTailer();
            Optional<Long> lastPositionOptional = getLastPosition();
            if (lastPositionOptional.isPresent()) {
                Long position = lastPositionOptional.get();
                long beginPosition = position + 1;
                tailer.moveToIndex(beginPosition);
                logDebug("[initExcerptTailerInternal] find last position and move to position: {}", beginPosition);
            } else {
                ConsumeFromWhere consumeFromWhere = this.config.getConsumeFromWhere();
                if (consumeFromWhere == ConsumeFromWhere.LAST) {
                    tailer.toEnd();
                    logDebug("[initExcerptTailerInternal] move to end");
                } else if (consumeFromWhere == ConsumeFromWhere.FIRST) {
                    tailer.toStart();
                    logDebug("[initExcerptTailerInternal] move to start");
                }
            }
            return tailer;
        } finally {
            logDebug("[initExcerptTailer] end");
        }

    }

    /// region position

    private void flushPosition() {
        if (ackedReadPosition != -1) {
            setLastPosition(this.ackedReadPosition);
        }
    }

    private Optional<Long> getLastPosition() {
        Long position = positionStore.get(config.getConsumerId());
        if (position == null) {
            return Optional.empty();
        }
        return Optional.of(position);
    }

    private void setLastPosition(long position) {
        positionStore.put(config.getConsumerId(), position);
    }

    /// endregion

    @Override
    public void close() {
        logDebug("[close] start");
        try {
            isClosing = true;
            this.internalLock.lock();
            stopReadToCache();
            this.internalLock.unlock();
            if (!positionStore.isClosed()) {
                positionStore.close();
            }
            scheduler.shutdown();
            readCacheExecutor.shutdown();
            try {
                if (!scheduler.awaitTermination(1, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
                if (!readCacheExecutor.awaitTermination(1, TimeUnit.SECONDS)) {
                    readCacheExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                readCacheExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            if (!queue.isClosed()) {
                queue.close();
            }

            for (CloseListener closeListener : closeListenerList) {
                closeListener.onClose();
            }
            isClosed = true;
        } finally {
            logDebug("[close] end");
        }
    }

    @Override
    public void addCloseListener(CloseListener listener) {
        closeListenerList.add(listener);
    }

    private void logDebug(String format) {
        if (logger.isDebugEnabled()) {
            logger.debug(format);
        }
    }

    private void logDebug(String format, Object arg) {
        if (logger.isDebugEnabled()) {
            logger.debug(format, arg);
        }
    }
}
