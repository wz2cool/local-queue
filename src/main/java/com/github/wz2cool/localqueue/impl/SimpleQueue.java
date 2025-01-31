package com.github.wz2cool.localqueue.impl;

import com.github.wz2cool.localqueue.IConsumer;
import com.github.wz2cool.localqueue.IQueue;
import com.github.wz2cool.localqueue.event.CloseListener;
import com.github.wz2cool.localqueue.model.config.SimpleConsumerConfig;
import com.github.wz2cool.localqueue.model.config.SimpleProducerConfig;
import com.github.wz2cool.localqueue.model.config.SimpleQueueConfig;
import com.github.wz2cool.localqueue.model.enums.ConsumeFromWhere;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * simple queue
 *
 * @author frank
 */
public class SimpleQueue implements IQueue {

    private final SimpleQueueConfig config;
    private final SimpleProducer simpleProducer;
    private final Map<String, SimpleConsumer> consumerMap = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<CloseListener> closeListeners = new ConcurrentLinkedQueue<>();

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

    private SimpleProducer getProducer() {
        return new SimpleProducer(new SimpleProducerConfig.Builder()
                .setDataDir(config.getDataDir())
                .setKeepDays(config.getKeepDays())
                .build());
    }

    @Override
    public synchronized IConsumer getConsumer(final String consumerId) {
        return getConsumer(consumerId, ConsumeFromWhere.LAST);
    }

    @Override
    public synchronized IConsumer getConsumer(final String consumerId, final ConsumeFromWhere consumeFromWhere) {
        SimpleConsumer consumer = consumerMap.get(consumerId);
        if (Objects.nonNull(consumer)) {
            return consumer;
        }

        consumer = new SimpleConsumer(new SimpleConsumerConfig.Builder()
                .setDataDir(config.getDataDir())
                .setConsumerId(consumerId)
                .setConsumeFromWhere(consumeFromWhere)
                .build());
        consumer.addCloseListener(() -> consumerMap.remove(consumerId));
        consumerMap.put(consumerId, consumer);
        return consumer;
    }

    @Override
    public void close() {
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
    }

    @Override
    public void addCloseListener(CloseListener listener) {
        closeListeners.add(listener);
    }
}
