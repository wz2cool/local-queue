package com.github.wz2cool.localqueue.impl;

import com.github.wz2cool.localqueue.IStore;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.map.ChronicleMap;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * 位置存储
 *
 * @author frank
 */
public class PositionStore implements IStore<Long>, AutoCloseable {

    private final ChronicleMap<String, Long> map;

    /**
     * 构造函数
     *
     * @param storeFile 存储文件
     */
    public PositionStore(final File storeFile) {
        try {
            Path dir = storeFile.toPath().getParent();
            if (!Files.exists(dir)) {
                Files.createDirectories(dir);
            }
            this.map = ChronicleMap.of(String.class, Long.class)
                    .name(storeFile.getName())
                    .averageKeySize(64)
                    .entries(10000)
                    .createPersistedTo(storeFile);
        } catch (Exception ex) {
            throw new IORuntimeException("[PositionStore.constructor] error", ex);
        }
    }

    @Override
    public void put(String key, Long value) {
        this.map.put(key, value);
    }

    @Override
    public Long get(String key) {
        return this.map.get(key);
    }

    @Override
    public void close() {
        this.map.close();
    }
}
