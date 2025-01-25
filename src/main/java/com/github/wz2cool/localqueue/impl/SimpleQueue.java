package com.github.wz2cool.localqueue.impl;

import com.github.wz2cool.localqueue.IQueue;
import com.github.wz2cool.localqueue.model.config.SimpleQueueConfig;
import com.github.wz2cool.localqueue.model.config.SimpleReaderConfig;
import com.github.wz2cool.localqueue.model.config.SimpleWriterConfig;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleQueue implements IQueue, AutoCloseable {

    private final SimpleQueueConfig config;
    private final SimpleWriter writer;
    private final Map<String, SimpleReader> readerMap = new ConcurrentHashMap<>();

    public SimpleQueue(SimpleQueueConfig config) {
        this.config = config;
        this.writer = getWriter();
    }

    @Override
    public boolean offer(String message) {
        return writer.offer(message);
    }


    private SimpleWriter getWriter() {
        return new SimpleWriter(new SimpleWriterConfig.Builder()
                .setDataDir(config.getDataDir())
                .setKeepDays(config.getKeepDays())
                .build());
    }

    @Override
    public synchronized SimpleReader getReader(final String readerKey) {
        SimpleReader reader = readerMap.get(readerKey);
        if (Objects.nonNull(reader)) {
            return reader;
        }

        reader = new SimpleReader(new SimpleReaderConfig.Builder()
                .setDataDir(config.getDataDir())
                .setReaderKey(readerKey)
                .build());
        readerMap.put(readerKey, reader);
        return reader;
    }

    @Override
    public void close() {
        writer.close();
        readerMap.forEach((k, v) -> v.close());
    }
}
