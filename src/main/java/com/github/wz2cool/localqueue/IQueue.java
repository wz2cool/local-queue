package com.github.wz2cool.localqueue;

import com.github.wz2cool.localqueue.model.message.QueueMessage;

import java.util.List;

public interface IQueue {
    boolean offer(String message);

    QueueMessage take(String readerKey) throws InterruptedException;

    List<QueueMessage> batchTake(String readerKey, int maxBatchSize) throws InterruptedException;

    void ack(String readerKey, List<QueueMessage> messages);

    void ack(String readerKey, long position);
}
