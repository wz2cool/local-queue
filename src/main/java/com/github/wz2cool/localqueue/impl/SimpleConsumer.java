package com.github.wz2cool.localqueue.impl;

import com.github.wz2cool.localqueue.IConsumer;
import com.github.wz2cool.localqueue.event.CloseListener;
import com.github.wz2cool.localqueue.helper.ChronicleQueueHelper;
import com.github.wz2cool.localqueue.model.config.SimpleConsumerConfig;
import com.github.wz2cool.localqueue.model.enums.ConsumeFromWhere;
import com.github.wz2cool.localqueue.model.message.InternalMessage;
import com.github.wz2cool.localqueue.model.message.QueueMessage;
import com.github.wz2cool.localqueue.model.page.PageInfo;
import com.github.wz2cool.localqueue.model.page.SortDirection;
import com.github.wz2cool.localqueue.model.page.UpDown;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.TailerDirection;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * simple consumer
 *
 * @author frank
 */
public class SimpleConsumer implements IConsumer {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final RollCycle defaultRollCycle;
    private final TimeProvider timeProvider;
    private final Set<String> matchTags;
    private final SimpleConsumerConfig config;
    private final PositionStore positionStore;
    private final SingleChronicleQueue queue;
    // should only call by readCacheExecutor
    private final ExcerptTailer mainTailer;
    private final ExecutorService readCacheExecutor = Executors.newSingleThreadExecutor();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final LinkedBlockingQueue<QueueMessage> messageCache;
    private final ConcurrentLinkedQueue<CloseListener> closeListenerList = new ConcurrentLinkedQueue<>();
    private final AtomicLong ackedReadPosition = new AtomicLong(-1);
    private final AtomicBoolean isReadToCacheRunning = new AtomicBoolean(true);
    private final AtomicBoolean isClosing = new AtomicBoolean(false);
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final Object closeLocker = new Object();
    private final AtomicInteger positionVersion = new AtomicInteger(0);

    /**
     * constructor
     *
     * @param config the config of consumer
     */
    public SimpleConsumer(final SimpleConsumerConfig config) {
        this.config = config;
        this.matchTags = getMatchTags(config.getSelectorTag());
        this.timeProvider = ChronicleQueueHelper.getTimeProvider(config.getTimeZone());
        this.messageCache = new LinkedBlockingQueue<>(config.getCacheSize());
        this.positionStore = new PositionStore(config.getPositionFile());
        this.defaultRollCycle = ChronicleQueueHelper.getRollCycle(config.getRollCycleType());
        this.queue = ChronicleQueue.singleBuilder(config.getDataDir())
                .timeProvider(timeProvider)
                .rollCycle(defaultRollCycle)
                .build();
        this.mainTailer = initMainTailer();
        startReadToCache();
        scheduler.scheduleAtFixedRate(this::flushPosition, 0, config.getFlushPositionInterval(), TimeUnit.MILLISECONDS);
    }


    private final List<QueueMessage> pendingMessages = Collections.synchronizedList(new ArrayList<>());

    @Override
    public synchronized QueueMessage take() throws InterruptedException {
        if (!pendingMessages.isEmpty()) {
            return pendingMessages.get(0);
        }
        QueueMessage message = this.messageCache.take();
        pendingMessages.add(message);
        return message;
    }

    @Override
    public synchronized List<QueueMessage> batchTake(int maxBatchSize) throws InterruptedException {
        if (!pendingMessages.isEmpty()) {
            return pendingMessages.subList(0, Math.min(maxBatchSize, pendingMessages.size()));
        }
        List<QueueMessage> result = new ArrayList<>(maxBatchSize);
        QueueMessage take = this.messageCache.take();
        result.add(take);
        this.messageCache.drainTo(result, maxBatchSize - 1);
        pendingMessages.addAll(result);
        return result;
    }

    @Override
    public synchronized Optional<QueueMessage> take(long timeout, TimeUnit unit) throws InterruptedException {
        if (!pendingMessages.isEmpty()) {
            return Optional.of(pendingMessages.get(0));
        }
        QueueMessage message = this.messageCache.poll(timeout, unit);
        if (Objects.nonNull(message)) {
            pendingMessages.add(message);
        }
        return Optional.ofNullable(message);
    }

    @Override
    public synchronized List<QueueMessage> batchTake(int maxBatchSize, long timeout, TimeUnit unit) throws InterruptedException {
        if (!pendingMessages.isEmpty()) {
            return pendingMessages.subList(0, Math.min(maxBatchSize, pendingMessages.size()));
        }
        List<QueueMessage> result = new ArrayList<>(maxBatchSize);
        QueueMessage poll = this.messageCache.poll(timeout, unit);
        if (Objects.nonNull(poll)) {
            result.add(poll);
            this.messageCache.drainTo(result, maxBatchSize - 1);
            pendingMessages.addAll(result);
        }
        return result;
    }

    @Override
    public synchronized Optional<QueueMessage> poll() {
        if (!pendingMessages.isEmpty()) {
            return Optional.of(pendingMessages.get(0));
        }
        QueueMessage message = this.messageCache.poll();
        if (Objects.nonNull(message)) {
            pendingMessages.add(message);
        }
        return Optional.ofNullable(message);
    }

    @Override
    public synchronized List<QueueMessage> batchPoll(int maxBatchSize) {
        if (!pendingMessages.isEmpty()) {
            return pendingMessages.subList(0, Math.min(maxBatchSize, pendingMessages.size()));
        }
        List<QueueMessage> result = new ArrayList<>(maxBatchSize);
        this.messageCache.drainTo(result, maxBatchSize);
        pendingMessages.addAll(result);
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
        ackedReadPosition.set(message.getPosition());
        pendingMessages.remove(message);
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
        ackedReadPosition.set(lastOne.getPosition());
        pendingMessages.removeAll(messages);
    }

    @Override
    public boolean moveToPosition(final long position) {
        logDebug("[moveToPosition] start");
        stopReadToCache();
        try {
            return moveToPositionInternal(position);
        } finally {
            startReadToCache();
            logDebug("[moveToPosition] end");
        }
    }

    @Override
    public boolean moveToTimestamp(final long timestamp) {
        logDebug("[moveToTimestamp] start, timestamp: {}", timestamp);
        stopReadToCache();
        try {
            Optional<Long> positionOptional = findPosition(timestamp);
            if (!positionOptional.isPresent()) {
                return false;
            }
            Long position = positionOptional.get();
            boolean moveToResult = moveToPositionInternal(position);
            logger.info("[moveToTimestamp] timestamp: {}, moveToResult: {}", timestamp, moveToResult);
            return moveToResult;
        } finally {
            startReadToCache();
            logDebug("[moveToTimestamp] end");
        }
    }

    @Override
    public Optional<QueueMessage> get(final long position) {
        if (position < 0) {
            return Optional.empty();
        }
        try (ExcerptTailer tailer = queue.createTailer()) {
            tailer.moveToIndex(position);
            InternalMessage InternalMessage = new InternalMessage();
            boolean readResult = tailer.readBytes(InternalMessage);
            if (readResult) {
                return Optional.of(toQueueMessage(InternalMessage, position));
            } else {
                return Optional.empty();
            }
        }
    }

    @Override
    public Optional<QueueMessage> get(final String messageKey, long searchTimestampStart, long searchTimestampEnd) {
        if (messageKey == null || messageKey.isEmpty()) {
            return Optional.empty();
        }
        // reuse this message
        InternalMessage InternalMessage = new InternalMessage();
        try (ExcerptTailer tailer = queue.createTailer()) {
            moveToNearByTimestamp(tailer, searchTimestampStart);
            while (true) {
                // for performance, ignore read content.
                boolean readResult = tailer.readBytes(InternalMessage);
                if (!readResult) {
                    return Optional.empty();
                }
                if (InternalMessage.getWriteTime() < searchTimestampStart) {
                    continue;
                }
                if (InternalMessage.getWriteTime() > searchTimestampEnd) {
                    return Optional.empty();
                }
                boolean moveToResult = tailer.moveToIndex(tailer.lastReadIndex());
                if (!moveToResult) {
                    return Optional.empty();
                }
                readResult = tailer.readBytes(InternalMessage);
                if (!readResult) {
                    return Optional.empty();
                }
                QueueMessage queueMessage = toQueueMessage(InternalMessage, tailer.lastReadIndex());
                if (Objects.equals(messageKey, queueMessage.getMessageKey())) {
                    return Optional.of(queueMessage);
                }
            }
        }
    }

    private Set<String> getMatchTags(String selectorTag) {
        logDebug("[getMatchTags] start, selectorTag: {}", selectorTag);
        ConcurrentHashMap.KeySetView<String, Boolean> mySet = ConcurrentHashMap.newKeySet();
        if (selectorTag == null || selectorTag.isEmpty()) {
            return mySet;
        }

        String[] tags = selectorTag.split("\\|\\|");
        mySet.addAll(Arrays.asList(tags));
        return mySet;
    }

    private QueueMessage toQueueMessage(final InternalMessage InternalMessage, final long position) {
        return new QueueMessage(
                InternalMessage.getTag(),
                InternalMessage.getMessageKey(),
                positionVersion.get(),
                position,
                InternalMessage.getContent(),
                InternalMessage.getWriteTime());
    }

    private boolean moveToPositionInternal(final long position) {
        return CompletableFuture.supplyAsync(() -> {
            synchronized (closeLocker) {
                try {
                    if (isClosing.get()) {
                        logDebug("[moveToPositionInternal] consumer is closing");
                        return false;
                    }
                    logDebug("[moveToPositionInternal] start, position: {}", position);
                    boolean moveToResult = mainTailer.moveToIndex(position);
                    if (moveToResult) {
                        positionVersion.incrementAndGet();
                        messageCache.clear();
                        ackedReadPosition.set(position);
                    }
                    logger.info("[local-queue] move to position: {}, result: {}", position, moveToResult);
                    return moveToResult;
                } finally {
                    logDebug("[moveToPositionInternal] end");
                }
            }
        }, this.readCacheExecutor).join();
    }


    @Override
    public Optional<Long> findPosition(final long timestamp) {
        logDebug("[findPosition] start, timestamp: {}", timestamp);
        try (ExcerptTailer tailer = queue.createTailer()) {
            moveToNearByTimestamp(tailer, timestamp);
            // reuse this message.
            InternalMessage InternalMessage = new InternalMessage(true);
            while (true) {
                boolean resultResult = tailer.readBytes(InternalMessage);
                if (resultResult) {
                    if (InternalMessage.getWriteTime() >= timestamp) {
                        return Optional.of(tailer.lastReadIndex());
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
        return ackedReadPosition.get();
    }

    @Override
    public boolean isClosed() {
        return isClosed.get();
    }

    private void stopReadToCache() {
        isReadToCacheRunning.set(false);
    }

    private void startReadToCache() {
        this.isReadToCacheRunning.set(true);
        readCacheExecutor.execute(this::readToCache);
    }

    private void readToCache() {
        try {
            logDebug("[readToCache] start");
            long pullInterval = config.getPullInterval();
            long fillCacheInterval = config.getFillCacheInterval();
            // reuse this message.
            InternalMessage InternalMessage = new InternalMessage(this.matchTags);
            while (isReadToCacheRunning.get()) {
                synchronized (closeLocker) {
                    try {
                        if (isClosing.get()) {
                            logDebug("[readToCache] consumer is closing");
                            return;
                        }

                        boolean readResult = mainTailer.readBytes(InternalMessage);
                        if (!readResult) {
                            TimeUnit.MILLISECONDS.sleep(pullInterval);
                            continue;
                        }
                        String messageTag = InternalMessage.getTag() == null ? "*" : InternalMessage.getTag();
                        if (matchTags.contains("*") || matchTags.contains(messageTag)) {
                            long lastedReadIndex = mainTailer.lastReadIndex();
                            QueueMessage queueMessage = toQueueMessage(InternalMessage, lastedReadIndex);
                            boolean offerResult = this.messageCache.offer(queueMessage, fillCacheInterval, TimeUnit.MILLISECONDS);
                            if (!offerResult) {
                                // if offer failed, move to last read position
                                mainTailer.moveToIndex(lastedReadIndex);
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        logger.error("[local-queue] read to cache error", e);
                    }
                }
            }
        } finally {
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
        try {
            if (ackedReadPosition.get() != -1) {
                setLastPosition(this.ackedReadPosition.get());
            }
        } catch (Exception e) {
            logger.error("flushPosition Exception", e);
        }
    }

    private Optional<Long> getLastPosition() {
        return positionStore.get(config.getConsumerId());
    }

    private void setLastPosition(long position) {
        positionStore.put(config.getConsumerId(), position);
    }

    /// endregion

    @SuppressWarnings("Duplicates")
    @Override
    public void close() {
        synchronized (closeLocker) {
            try {
                logDebug("[close] start");
                if (isClosing.get()) {
                    logDebug("[close] is closing");
                    return;
                }
                isClosing.set(true);
                stopReadToCache();
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
                isClosed.set(true);
            } finally {
                logDebug("[close] end");
            }
        }
    }

    private void moveToNearByTimestamp(ExcerptTailer tailer, long timestamp) {
        int expectedCycle = ChronicleQueueHelper.cycle(defaultRollCycle, timeProvider, timestamp);
        int currentCycle = tailer.cycle();
        if (currentCycle != expectedCycle) {
            boolean moveToCycleResult = tailer.moveToCycle(expectedCycle);
            logDebug("[moveToNearByTimestamp] moveToCycleResult: {}", moveToCycleResult);
        }
    }

    @Override
    public void addCloseListener(CloseListener listener) {
        closeListenerList.add(listener);
    }

    // region page

    @Override
    public PageInfo<QueueMessage> getPage(SortDirection sortDirection, int pageSize) {
        return getPage(-1, sortDirection, pageSize);
    }

    @SuppressWarnings("Duplicates")
    @Override
    public PageInfo<QueueMessage> getPage(long moveToPosition, SortDirection sortDirection, int pageSize) {
        try (ExcerptTailer tailer = queue.createTailer()) {
            if (moveToPosition != -1) {
                tailer.moveToIndex(moveToPosition);
            }
            if (sortDirection == SortDirection.DESC) {
                tailer.toEnd();
                tailer.direction(TailerDirection.BACKWARD);
            }
            List<QueueMessage> data = new ArrayList<>();
            long start = -1;
            long end = -1;
            // reuse this message.
            InternalMessage InternalMessage = new InternalMessage();
            for (int i = 0; i < pageSize; i++) {
                boolean readResult = tailer.readBytes(InternalMessage);
                if (!readResult) {
                    break;
                }
                QueueMessage queueMessage = toQueueMessage(InternalMessage, tailer.lastReadIndex());
                data.add(queueMessage);
                if (i == 0) {
                    start = tailer.lastReadIndex();
                }
                end = tailer.lastReadIndex();
            }
            return new PageInfo<>(start, end, data, sortDirection, pageSize);
        }
    }

    @SuppressWarnings("Duplicates")
    @Override
    public PageInfo<QueueMessage> getPage(PageInfo<QueueMessage> prevPageInfo, UpDown upDown) {
        SortDirection sortDirection = prevPageInfo.getSortDirection();
        int pageSize = prevPageInfo.getPageSize();
        long start = prevPageInfo.getStart();
        long end = prevPageInfo.getEnd();
        try (ExcerptTailer tailer = queue.createTailer()) {
            TailerDirection tailerDirection = getTailerDirection(sortDirection, upDown);
            tailer.direction(tailerDirection);
            if (sortDirection == SortDirection.DESC) {
                if (upDown == UpDown.DOWN) {
                    tailer.moveToIndex(end - 1);
                } else {
                    tailer.moveToIndex(start + 1);
                }
            } else {
                if (upDown == UpDown.DOWN) {
                    tailer.moveToIndex(end + 1);
                } else {
                    tailer.moveToIndex(start - 1);
                }
            }
            List<QueueMessage> data = new ArrayList<>();
            // reuse this message.
            InternalMessage InternalMessage = new InternalMessage();
            for (int i = 0; i < pageSize; i++) {
                boolean readResult = tailer.readBytes(InternalMessage);
                if (!readResult) {
                    break;
                }
                QueueMessage queueMessage = toQueueMessage(InternalMessage, tailer.lastReadIndex());
                data.add(queueMessage);
                if (i == 0) {
                    start = tailer.lastReadIndex();
                }
                end = tailer.lastReadIndex();
            }
            if (upDown == UpDown.UP) {
                Collections.reverse(data);
            }
            return new PageInfo<>(start, end, data, sortDirection, pageSize);
        }
    }

    private TailerDirection getTailerDirection(SortDirection sortDirection, UpDown upDown) {
        if (sortDirection == SortDirection.DESC && upDown == UpDown.DOWN) {
            return TailerDirection.BACKWARD;
        }
        if (sortDirection == SortDirection.DESC && upDown == UpDown.UP) {
            return TailerDirection.FORWARD;
        }
        if (sortDirection == SortDirection.ASC && upDown == UpDown.DOWN) {
            return TailerDirection.FORWARD;
        }
        if (sortDirection == SortDirection.ASC && upDown == UpDown.UP) {
            return TailerDirection.BACKWARD;
        }
        return TailerDirection.FORWARD;
    }

    // endregion

    // region logger

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

    // endregion
}
