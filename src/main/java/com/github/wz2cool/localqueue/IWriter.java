package com.github.wz2cool.localqueue;

/**
 * writer interface.
 *
 * @author Frank
 */
public interface IWriter {

    /**
     * offer message to queue.
     *
     * @param message message
     * @return true if success
     */
    boolean offer(String message);
}
