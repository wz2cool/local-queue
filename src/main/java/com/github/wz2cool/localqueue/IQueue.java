package com.github.wz2cool.localqueue;

import com.github.wz2cool.localqueue.model.enums.ConsumeFromWhere;

/**
 * queue interface
 *
 * @author Frank
 */
public interface IQueue {

    /**
     * offer message to queue
     *
     * @param message message
     * @return true if success
     */
    boolean offer(String message);

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
}
