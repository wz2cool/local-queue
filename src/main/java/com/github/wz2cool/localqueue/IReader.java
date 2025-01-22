package com.github.wz2cool.localqueue;

import com.github.wz2cool.localqueue.model.message.QueueMessage;

import java.util.List;

public interface IReader {

    QueueMessage blockingRead();

    List<QueueMessage> blockingBatchRead(int maxBatchSize);

    void ack(long position);

    void ack(List<QueueMessage> messages);
}
