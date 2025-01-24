package com.github.wz2cool.localqueue.model.config;

import java.io.File;
import java.util.Objects;

public class SimpleReaderConfig {

    private final File dataDir;

    private final File positionFile;

    private final String readerKey;

    private final long pullInterval;

    private final int readCacheSize;

    private final long flushPositionInterval;

    private SimpleReaderConfig(final Builder builder) {
        this.dataDir = builder.dataDir;
        this.positionFile = builder.positionFile;
        this.readerKey = builder.readerKey;
        this.pullInterval = builder.pullInterval;
        this.readCacheSize = builder.readCacheSize;
        this.flushPositionInterval = builder.flushPositionInterval;
    }

    public File getDataDir() {
        return dataDir;
    }

    public File getPositionFile() {
        return positionFile;
    }

    public String getReaderKey() {
        return readerKey;
    }

    public long getPullInterval() {
        return pullInterval;
    }

    public int getReadCacheSize() {
        return readCacheSize;
    }

    public long getFlushPositionInterval() {
        return flushPositionInterval;
    }

    public static class Builder {

        private File dataDir;

        private File positionFile;

        private String readerKey;

        private long pullInterval = 500;

        private int readCacheSize = 10000;

        private long flushPositionInterval = 500;

        public Builder setDataDir(File dataDir) {
            this.dataDir = dataDir;
            return this;
        }

        public Builder setPositionFile(File positionFile) {
            this.positionFile = positionFile;
            return this;
        }

        public Builder setReaderKey(String readerKey) {
            this.readerKey = readerKey;
            return this;
        }

        public Builder setPullInterval(long pullInterval) {
            this.pullInterval = pullInterval;
            return this;
        }

        public Builder setReadCacheSize(int readCacheSize) {
            this.readCacheSize = readCacheSize;
            return this;
        }

        public Builder setFlushPositionInterval(long flushPositionInterval) {
            this.flushPositionInterval = flushPositionInterval;
            return this;
        }

        public SimpleReaderConfig build() {
            if (Objects.isNull(dataDir)) {
                throw new IllegalArgumentException("dataDir cannot be null");
            }

            if (Objects.isNull(readerKey) || readerKey.isEmpty()) {
                // 如果没有就给默认
                throw new IllegalArgumentException("readerKey cannot be null or empty");
            }

            if (readCacheSize <= 0) {
                throw new IllegalArgumentException("readCacheCount should > 0");
            }

            if (flushPositionInterval <= 0) {
                throw new IllegalArgumentException("flushPositionInterval should > 0");
            }

            if (Objects.isNull(positionFile)) {
                // 如果没有就给默认
                this.positionFile = new File(dataDir, "position.dat");
            }

            return new SimpleReaderConfig(this);
        }

    }
}
