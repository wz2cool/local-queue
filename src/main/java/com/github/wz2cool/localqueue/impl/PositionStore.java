package com.github.wz2cool.localqueue.impl;

import com.github.wz2cool.localqueue.IStore;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.map.ChronicleMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 位置存储
 *
 * @author frank
 */
public class PositionStore implements IStore<String, Long> {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final ChronicleMap<String, Long> map;

    private final AtomicBoolean isClosing = new AtomicBoolean(false);
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final Object closeLocker = new Object();

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
    public boolean isClosed() {
        return this.isClosed.get();
    }

    @Override
    public void put(String key, Long value) {
        if (isClosing.get()) {
            throw new IORuntimeException("PositionStore is closing");
        }
        this.map.put(key, value);
    }

    @Override
    public Optional<Long> get(String key) {
        if (isClosing.get()) {
            throw new IORuntimeException("PositionStore is closing");
        }
        return Optional.ofNullable(this.map.get(key));
    }

    @Override
    public synchronized void close() {
        synchronized (closeLocker) {
            try {
                logDebug("[close] start");
                if (isClosing.get()) {
                    logDebug("[close] is closing");
                    return;
                }
                isClosing.set(true);
                if (!this.map.isClosed()) {
                    this.map.close();
                }
                isClosed.set(true);
            } finally {
                logDebug("[close] end");
            }
        }
    }

    // region logger

    private void logDebug(String format) {
        if (logger.isDebugEnabled()) {
            logger.debug(format);
        }
    }

    // endregion
}
