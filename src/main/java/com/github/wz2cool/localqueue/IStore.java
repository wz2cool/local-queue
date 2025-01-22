package com.github.wz2cool.localqueue;

import java.io.Serializable;

/**
 * 存储
 *
 * @param <T> 泛型
 */
public interface IStore<T extends Serializable> {

    /**
     * 放置
     *
     * @param key   键
     * @param value 值
     */
    void put(String key, T value);

    /**
     * 获取
     *
     * @param key 键
     * @return 值
     */
    T get(String key);
}
