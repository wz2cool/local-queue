package com.github.wz2cool.localqueue;

public interface IQueue {
    boolean offer(String message);

    IReader getReader(String readerKey);
}
