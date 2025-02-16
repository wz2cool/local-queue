package com.github.wz2cool.localqueue.impl;

import com.github.wz2cool.localqueue.IConsumer;
import com.github.wz2cool.localqueue.IQueue;
import com.github.wz2cool.localqueue.event.CloseListener;
import com.github.wz2cool.localqueue.model.config.SimpleConsumerConfig;
import com.github.wz2cool.localqueue.model.config.SimpleProducerConfig;
import com.github.wz2cool.localqueue.model.config.SimpleQueueConfig;
import com.github.wz2cool.localqueue.model.enums.ConsumeFromWhere;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * simple queue
 *
 * @author frank
 */
public class SimpleQueue implements IQueue {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final SimpleQueueConfig config;
    private final SimpleProducer simpleProducer;
    private final Map<String, SimpleConsumer> consumerMap = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<CloseListener> closeListeners = new ConcurrentLinkedQueue<>();

    private final Object closeLocker = new Object();
    private final AtomicBoolean isClosing = new AtomicBoolean(false);
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    public SimpleQueue(SimpleQueueConfig config) {
        this.config = config;
        this.simpleProducer = getProducer();
    }

    @Override
    public boolean offer(String message) {
        return simpleProducer.offer(message);
    }

    @Override
    public boolean offer(String messageKey, String message) {
        return simpleProducer.offer(messageKey, message);
    }

    @Override
    public boolean isClosed() {
        return isClosed.get();
    }

    private SimpleProducer getProducer() {
        return new SimpleProducer(new SimpleProducerConfig.Builder()
                .setDataDir(config.getDataDir())
                .setKeepDays(config.getKeepDays())
                .setRollCycleType(config.getRollCycleType())
                .setTimeZone(config.getTimeZone())
                .build());
    }

    @Override
    public synchronized IConsumer getConsumer(final String consumerId) {
        return getConsumer(consumerId, ConsumeFromWhere.LAST);
    }

    public synchronized IConsumer getConsumer(final String consumerId, final ConsumeFromWhere consumeFromWhere) {
        return getConsumer(consumerId, null, consumeFromWhere);
    }

    @Override
    public synchronized IConsumer getConsumer(final String consumerId, final String selectTag, final ConsumeFromWhere consumeFromWhere) {
        SimpleConsumer consumer = consumerMap.get(consumerId);
        if (Objects.nonNull(consumer)) {
            return consumer;
        }

        consumer = new SimpleConsumer(new SimpleConsumerConfig.Builder()
                .setDataDir(config.getDataDir())
                .setConsumerId(consumerId)
                .setConsumeFromWhere(consumeFromWhere)
                .setRollCycleType(config.getRollCycleType())
                .setTimeZone(config.getTimeZone())
                .setSelectorTag(selectTag)
                .build());
        consumer.addCloseListener(() -> {
            SimpleConsumer removeItem = consumerMap.remove(consumerId);
            removeItem.close();
        });
        consumerMap.put(consumerId, consumer);
        return consumer;
    }

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
                if (!simpleProducer.isClosed()) {
                    simpleProducer.close();
                }
                for (Map.Entry<String, SimpleConsumer> entry : consumerMap.entrySet()) {
                    SimpleConsumer consumer = entry.getValue();
                    if (!consumer.isClosed()) {
                        entry.getValue().close();
                    }
                }
                for (CloseListener listener : closeListeners) {
                    listener.onClose();
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

    // region logger

    private void logDebug(String format) {
        if (logger.isDebugEnabled()) {
            logger.debug(format);
        }
    }

    // endregion
}
