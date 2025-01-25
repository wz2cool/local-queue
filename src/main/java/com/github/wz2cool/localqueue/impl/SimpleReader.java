package com.github.wz2cool.localqueue.impl;

import com.github.wz2cool.localqueue.IReader;
import com.github.wz2cool.localqueue.model.config.SimpleReaderConfig;
import com.github.wz2cool.localqueue.model.message.QueueMessage;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * simple reader
 *
 * @author frank
 */
public class SimpleReader implements IReader, AutoCloseable {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final SimpleReaderConfig config;
    private final PositionStore positionStore;

    private final SingleChronicleQueue queue;
    private final ThreadLocal<ExcerptTailer> tailerThreadLocal;
    private final ExecutorService readExecutor = Executors.newSingleThreadExecutor();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final LinkedBlockingQueue<QueueMessage> messageCache;
    private volatile long ackedReadPosition = -1;
    private volatile boolean isReadToCacheRunning = true;

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
        this.tailerThreadLocal = ThreadLocal.withInitial(this::getExcerptTailer);
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
    public synchronized void ack(final List<QueueMessage> messages) {
        if (Objects.isNull(messages) || messages.isEmpty()) {
            return;
        }
        QueueMessage lastOne = messages.get(messages.size() - 1);
        this.ackedReadPosition = lastOne.getPosition();
    }

    private void stopReadToCache() {
        this.isReadToCacheRunning = false;
    }

    private void readToCache() {
        ExcerptTailer tailer = tailerThreadLocal.get();
        long pullInterval = config.getPullInterval();
        while (this.isReadToCacheRunning) {
            try {
                String message = tailer.readText();
                if (Objects.isNull(message)) {
                    TimeUnit.MILLISECONDS.sleep(pullInterval);
                    continue;
                }
                long lastedReadIndex = tailer.lastReadIndex();
                QueueMessage queueMessage = new QueueMessage(lastedReadIndex, message);
                this.messageCache.put(queueMessage);
            } catch (InterruptedException e) {
                logger.error("[readToCache] error", e);
                Thread.currentThread().interrupt();
            }
        }
    }

    private ExcerptTailer getExcerptTailer() {
        ExcerptTailer tailer = queue.createTailer();
        Optional<Long> lastPositionOptional = getLastPosition();
        if (lastPositionOptional.isPresent()) {
            Long position = lastPositionOptional.get();
            tailer.moveToIndex(position);
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
    public void close() {
        stopReadToCache();
        queue.close();
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
    }
}
