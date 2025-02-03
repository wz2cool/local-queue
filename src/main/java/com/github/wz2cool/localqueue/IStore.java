package com.github.wz2cool.localqueue;

import java.io.Serializable;
import java.util.Optional;

/**
 * store interface
 *
 * @param <TValue> valueType
 */
public interface IStore<TKey extends Serializable, TValue extends Serializable> extends AutoCloseable {

    /**
     * is store closed
     *
     * @return is store closed
     */
    boolean isClosed();

    /**
     * put key value
     *
     * @param key   key
     * @param value value
     */
    void put(TKey key, TValue value);

    /**
     * get value by key
     *
     * @param key key
     * @return value
     */
    Optional<TValue> get(String key);

    /**
     * close store
     */
    void close();
}
