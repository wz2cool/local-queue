package com.github.wz2cool.localqueue;

import com.github.wz2cool.localqueue.event.CloseListener;
import com.github.wz2cool.localqueue.model.enums.ConsumeFromWhere;

/**
 * queue interface
 *
 * @author Frank
 */
public interface IQueue extends AutoCloseable {

    /**
     * offer message to queue
     *
     * @param message message
     * @return true if success
     */
    boolean offer(String message);

    /**
     * offer message to queue
     *
     * @param messageKey message key
     * @param message    message
     * @return true if success
     */
    boolean offer(String messageKey, String message);

    /**
     * get consumer
     *
     * @param consumerId consumer id
     * @return consumer
     */
    IConsumer getConsumer(String consumerId);

    /**
     * get consumer
     *
     * @param consumerId       consumer id
     * @param consumeFromWhere consume from where
     * @return consumer
     */
    IConsumer getConsumer(String consumerId, ConsumeFromWhere consumeFromWhere);

    /**
     * close queue
     */
    void close();

    /**
     * add close listener.
     *
     * @param listener close listener
     */
    void addCloseListener(CloseListener listener);
}
