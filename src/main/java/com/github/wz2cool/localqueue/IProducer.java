package com.github.wz2cool.localqueue;

/**
 * producer interface.
 *
 * @author Frank
 */
public interface IProducer {

    /**
     * offer message to queue.
     *
     * @param message message
     * @return true if success
     */
    boolean offer(String message);

    /**
     * offer message to queue.
     *
     * @param messageKey message key
     * @param message    message
     * @return true if success
     */
    boolean offer(String messageKey, String message);
}
