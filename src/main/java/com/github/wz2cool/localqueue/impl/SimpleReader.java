package com.github.wz2cool.localqueue.impl;

import com.github.wz2cool.localqueue.IReader;
import com.github.wz2cool.localqueue.model.config.SimpleReaderConfig;
import com.github.wz2cool.localqueue.model.message.QueueMessage;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * simple reader
 *
 * @author frank
 */
public class SimpleReader implements IReader, AutoCloseable {

    private final SimpleReaderConfig config;
    private final PositionStore positionStore;

    private final SingleChronicleQueue queue;
    private final ThreadLocal<ExcerptTailer> tailerThreadLocal;
    private final ExecutorService readExecutor = Executors.newSingleThreadExecutor();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final LinkedBlockingQueue<QueueMessage> messageCache;
    private volatile long ackedReadPosition = -1;
    private volatile boolean isReadToCacheRunning = true;
    private volatile boolean isClosing = false;
    private volatile boolean isClosed = false;

    private final Lock internalLock = new ReentrantLock();


    /**
     * constructor
     *
     * @param config the config of reader
     */
    public SimpleReader(final SimpleReaderConfig config) {
        this.config = config;
        this.messageCache = new LinkedBlockingQueue<>(config.getReadCacheSize());
        this.positionStore = new PositionStore(config.getPositionFile());
        this.queue = ChronicleQueue.singleBuilder(config.getDataDir()).rollCycle(RollCycles.FAST_DAILY).build();
        this.tailerThreadLocal = ThreadLocal.withInitial(this::initExcerptTailer);
        scheduler.scheduleAtFixedRate(this::flushPosition, 0, config.getFlushPositionInterval(), TimeUnit.MILLISECONDS);
        readExecutor.execute(this::readToCache);
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
    public synchronized void ack(final long position) {
        this.ackedReadPosition = position;
    }

    @Override
    public synchronized void ack(final QueueMessage message) {
        if (Objects.isNull(message)) {
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
        this.ackedReadPosition = lastOne.getPosition();
    }

    @Override
    public synchronized boolean moveToPosition(final long position) {
        stopReadToCache();
        try {
            internalLock.lock();
            return CompletableFuture.supplyAsync(() -> {
                ExcerptTailer tailer = tailerThreadLocal.get();
                boolean moveToResultInternal = tailer.moveToIndex(position);
                if (moveToResultInternal) {
                    messageCache.clear();
                    this.ackedReadPosition = position;
                }
                return moveToResultInternal;
            }, this.readExecutor).join();
        } finally {
            internalLock.unlock();
            startReadToCache();
            readExecutor.execute(this::readToCache);
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
    }

    private void readToCache() {
        try {
            internalLock.lock();
            long pullInterval = config.getPullInterval();
            while (this.isReadToCacheRunning && !isClosing) {
                try {
                    ExcerptTailer tailer = tailerThreadLocal.get();
                    String message = tailer.readText();
                    if (Objects.isNull(message)) {
                        TimeUnit.MILLISECONDS.sleep(pullInterval);
                        continue;
                    }
                    long lastedReadIndex = tailer.lastReadIndex();
                    QueueMessage queueMessage = new QueueMessage(lastedReadIndex, message);
                    this.messageCache.put(queueMessage);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        } finally {
            internalLock.unlock();
        }
    }

    private ExcerptTailer initExcerptTailer() {
        ExcerptTailer tailer = queue.createTailer();
        Optional<Long> lastPositionOptional = getLastPosition();
        if (lastPositionOptional.isPresent()) {
            Long position = lastPositionOptional.get();
            long beginPosition = position + 1;
            tailer.moveToIndex(beginPosition);
        }
        return tailer;
    }

    /// region position

    private void flushPosition() {
        if (ackedReadPosition != -1) {
            setLastPosition(this.ackedReadPosition);
        }
    }

    private Optional<Long> getLastPosition() {
        Long position = positionStore.get(config.getReaderKey());
        if (position == null) {
            return Optional.empty();
        }
        return Optional.of(position);
    }

    private void setLastPosition(long position) {
        positionStore.put(config.getReaderKey(), position);
    }

    /// endregion

    @Override
    public synchronized void close() {
        isClosing = true;
        this.internalLock.lock();
        try {
            stopReadToCache();
            positionStore.close();
            scheduler.shutdown();
            readExecutor.shutdown();
            try {
                if (!scheduler.awaitTermination(1, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
                if (!readExecutor.awaitTermination(1, TimeUnit.SECONDS)) {
                    readExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                readExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            tailerThreadLocal.remove();
            queue.close();
            isClosed = true;
        } finally {
            this.internalLock.unlock();
        }
    }
}
