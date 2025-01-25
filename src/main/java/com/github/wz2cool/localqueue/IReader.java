package com.github.wz2cool.localqueue;

import com.github.wz2cool.localqueue.model.message.QueueMessage;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public interface IReader {

    QueueMessage take() throws InterruptedException;

    List<QueueMessage> batchTake(int maxBatchSize) throws InterruptedException;

    Optional<QueueMessage> take(long timeout, TimeUnit unit) throws InterruptedException;

    List<QueueMessage> batchTake(int maxBatchSize, long timeout, TimeUnit unit) throws InterruptedException;

    Optional<QueueMessage> poll();

    List<QueueMessage> batchPoll(int maxBatchSize);

    void ack(long position);

    void ack(List<QueueMessage> messages);
}
