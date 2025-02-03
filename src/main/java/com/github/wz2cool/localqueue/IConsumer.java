package com.github.wz2cool.localqueue;

import com.github.wz2cool.localqueue.event.CloseListener;
import com.github.wz2cool.localqueue.model.message.QueueMessage;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * consumer interface
 *
 * @author frank
 */
public interface IConsumer extends AutoCloseable {

    /**
     * blocking thread until message available.
     *
     * @return message
     * @throws InterruptedException if interrupted while waiting
     */
    QueueMessage take() throws InterruptedException;

    /**
     * blocking thread until messages available.
     *
     * @param maxBatchSize max batch size
     * @return the messages
     * @throws InterruptedException if interrupted while waiting
     */
    List<QueueMessage> batchTake(int maxBatchSize) throws InterruptedException;

    /**
     * blocking thread until message available.
     *
     * @param timeout return Optional.empty() if timeout
     * @param unit    time unit
     * @return message
     * @throws InterruptedException if interrupted while waiting
     */
    Optional<QueueMessage> take(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * blocking thread until messages available.
     *
     * @param maxBatchSize max batch size
     * @param timeout      return Optional.empty() if timeout
     * @param unit         time unit
     * @return the messages
     * @throws InterruptedException if interrupted while waiting
     */
    List<QueueMessage> batchTake(int maxBatchSize, long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * non-blocking thread.
     *
     * @return message
     */
    Optional<QueueMessage> poll();

    /**
     * non-blocking thread.
     *
     * @param maxBatchSize max batch size
     * @return the messages
     */
    List<QueueMessage> batchPoll(int maxBatchSize);

    /**
     * ack message.
     *
     * @param message message
     */
    void ack(QueueMessage message);

    /**
     * ack message.
     *
     * @param messages messages
     */
    void ack(List<QueueMessage> messages);

    /**
     * move to position.
     *
     * @param position position
     * @return true if success
     */
    boolean moveToPosition(long position);

    /**
     * move to timestamp.
     *
     * @param timestamp timestamp
     * @return true if success
     */
    boolean moveToTimestamp(long timestamp);


    /**
     * get message by messageKey.
     *
     * @param messageKey           messageKey
     * @param searchTimestampStart search timestamp start
     * @param searchTimestampEnd   search timestamp end
     * @return message
     */
    Optional<QueueMessage> get(String messageKey, long searchTimestampStart, long searchTimestampEnd);

    /**
     * is closed
     *
     * @return true if closed
     */
    boolean isClosed();

    /**
     * close consumer.
     */
    void close();

    /**
     * add close listener.
     *
     * @param listener close listener
     */
    void addCloseListener(CloseListener listener);
}
