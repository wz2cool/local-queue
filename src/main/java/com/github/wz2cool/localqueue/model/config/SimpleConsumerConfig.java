package com.github.wz2cool.localqueue.model.config;

import com.github.wz2cool.localqueue.model.enums.ConsumeFromWhere;

import java.io.File;
import java.util.Objects;

public class SimpleConsumerConfig {

    private final File dataDir;

    private final File positionFile;

    private final String consumerId;

    private final long pullInterval;

    private final int cacheSize;

    private final long flushPositionInterval;

    private final ConsumeFromWhere consumeFromWhere;

    private SimpleConsumerConfig(final Builder builder) {
        this.dataDir = builder.dataDir;
        this.positionFile = builder.positionFile;
        this.consumerId = builder.consumerId;
        this.pullInterval = builder.pullInterval;
        this.cacheSize = builder.cacheSize;
        this.flushPositionInterval = builder.flushPositionInterval;
        this.consumeFromWhere = builder.consumeFromWhere;
    }

    public File getDataDir() {
        return dataDir;
    }

    public File getPositionFile() {
        return positionFile;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public long getPullInterval() {
        return pullInterval;
    }

    public int getCacheSize() {
        return cacheSize;
    }

    public long getFlushPositionInterval() {
        return flushPositionInterval;
    }

    public ConsumeFromWhere getConsumeFromWhere() {
        return consumeFromWhere;
    }

    public static class Builder {

        private File dataDir;

        private File positionFile;

        private String consumerId;

        private long pullInterval = 10;

        private int cacheSize = 10000;

        private long flushPositionInterval = 100;

        private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.LAST;

        public Builder setDataDir(File dataDir) {
            this.dataDir = dataDir;
            return this;
        }

        public Builder setPositionFile(File positionFile) {
            this.positionFile = positionFile;
            return this;
        }

        public Builder setConsumerId(String consumerId) {
            this.consumerId = consumerId;
            return this;
        }

        public Builder setPullInterval(long pullInterval) {
            this.pullInterval = pullInterval;
            return this;
        }

        public Builder setCacheSize(int cacheSize) {
            this.cacheSize = cacheSize;
            return this;
        }

        public Builder setFlushPositionInterval(long flushPositionInterval) {
            this.flushPositionInterval = flushPositionInterval;
            return this;
        }

        public Builder setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
            this.consumeFromWhere = consumeFromWhere;
            return this;
        }

        public SimpleConsumerConfig build() {
            if (Objects.isNull(dataDir)) {
                throw new IllegalArgumentException("dataDir cannot be null");
            }

            if (Objects.isNull(consumerId) || consumerId.isEmpty()) {
                // 如果没有就给默认
                throw new IllegalArgumentException("consumerId cannot be null or empty");
            }

            if (cacheSize <= 0) {
                throw new IllegalArgumentException("cacheSize should > 0");
            }

            if (flushPositionInterval <= 0) {
                throw new IllegalArgumentException("flushPositionInterval should > 0");
            }

            if (Objects.isNull(positionFile)) {
                // 如果没有就给默认
                this.positionFile = new File(dataDir, "position.dat");
            }

            if (Objects.isNull(consumeFromWhere)) {
                throw new IllegalArgumentException("consumeFromWhere cannot be null");
            }


            return new SimpleConsumerConfig(this);
        }

    }
}
