package com.github.wz2cool.localqueue;

import com.github.wz2cool.localqueue.model.config.SimpleReaderConfig;

public interface IQueue {
    boolean offer(String message);

    IReader getReader(String readerKey);

    IReader getReader(SimpleReaderConfig config);
}
