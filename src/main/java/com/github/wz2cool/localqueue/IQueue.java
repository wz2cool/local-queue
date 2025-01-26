package com.github.wz2cool.localqueue;

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
     * get reader
     *
     * @param readerKey reader key
     * @return reader
     */
    IReader getReader(String readerKey);

}
