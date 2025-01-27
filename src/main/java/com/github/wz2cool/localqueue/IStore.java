package com.github.wz2cool.localqueue;

import java.io.Serializable;

/**
 * store interface
 *
 * @param <T> valueType
 */
public interface IStore<T extends Serializable> {

    /**
     * put key value
     *
     * @param key   key
     * @param value value
     */
    void put(String key, T value);

    /**
     * get value by key
     *
     * @param key key
     * @return value
     */
    T get(String key);
}
