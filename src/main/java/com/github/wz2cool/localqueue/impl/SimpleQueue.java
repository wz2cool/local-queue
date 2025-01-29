package com.github.wz2cool.localqueue.impl;

import com.github.wz2cool.localqueue.IQueue;
import com.github.wz2cool.localqueue.IConsumer;
import com.github.wz2cool.localqueue.model.config.SimpleQueueConfig;
import com.github.wz2cool.localqueue.model.config.SimpleConsumerConfig;
import com.github.wz2cool.localqueue.model.config.SimpleProducerConfig;
import com.github.wz2cool.localqueue.model.enums.ConsumeFromWhere;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * simple queue
 *
 * @author frank
 */
public class SimpleQueue implements IQueue, AutoCloseable {

    private final SimpleQueueConfig config;
    private final SimpleProducer simpleProducer;
    private final Map<String, SimpleConsumer> consumerMap = new ConcurrentHashMap<>();

    public SimpleQueue(SimpleQueueConfig config) {
        this.config = config;
        this.simpleProducer = getProducer();
    }

    @Override
    public boolean offer(String message) {
        return simpleProducer.offer(message);
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
        consumerMap.put(consumerId, consumer);
        return consumer;
    }

    @Override
    public void close() {
        simpleProducer.close();
        for (Map.Entry<String, SimpleConsumer> entry : consumerMap.entrySet()) {
            entry.getValue().close();
        }
    }
}
