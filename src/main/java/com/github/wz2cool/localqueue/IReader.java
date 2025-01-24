package com.github.wz2cool.localqueue;

import com.github.wz2cool.localqueue.model.message.QueueMessage;

import java.util.List;
import java.util.Optional;

public interface IReader {

    QueueMessage blockingRead() throws InterruptedException;

    List<QueueMessage> blockingBatchRead(int maxBatchSize) throws InterruptedException;

    Optional<QueueMessage> read();

    List<QueueMessage> readBatch(int maxBatchSize);

    void ack(long position);

    void ack(List<QueueMessage> messages);
}
