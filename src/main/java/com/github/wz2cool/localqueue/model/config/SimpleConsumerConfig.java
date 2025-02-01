package com.github.wz2cool.localqueue.model.config;

import com.github.wz2cool.localqueue.model.enums.ConsumeFromWhere;
import com.github.wz2cool.localqueue.model.enums.RollCycleType;

import java.io.File;
import java.util.Objects;
import java.util.TimeZone;

public class SimpleConsumerConfig {

    private final File dataDir;

    private final File positionFile;

    private final String consumerId;

    private final long pullInterval;

    private final long fillCacheInterval;

    private final int cacheSize;

    private final long flushPositionInterval;

    private final ConsumeFromWhere consumeFromWhere;

    private final RollCycleType rollCycleType;

    private final TimeZone timeZone;

    private SimpleConsumerConfig(final Builder builder) {
        this.dataDir = builder.dataDir;
        this.positionFile = builder.positionFile;
        this.consumerId = builder.consumerId;
        this.pullInterval = builder.pullInterval;
        this.fillCacheInterval = builder.fillCacheInterval;
        this.cacheSize = builder.cacheSize;
        this.flushPositionInterval = builder.flushPositionInterval;
        this.consumeFromWhere = builder.consumeFromWhere;
        this.rollCycleType = builder.rollCycleType;
        this.timeZone = builder.timeZone;
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

    public RollCycleType getRollCycleType() {
        return rollCycleType;
    }

    public long getFillCacheInterval() {
        return fillCacheInterval;
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    public static class Builder {

        private File dataDir;

        private File positionFile;

        private String consumerId;

        private long pullInterval = 10;

        private int cacheSize = 10000;

        private long fillCacheInterval = 500;

        private long flushPositionInterval = 100;

        private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.LAST;

        private RollCycleType rollCycleType = RollCycleType.HOURLY;

        private TimeZone timeZone = TimeZone.getDefault();

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

        public Builder setRollCycleType(RollCycleType rollCycleType) {
            this.rollCycleType = rollCycleType;
            return this;
        }

        public Builder setFillCacheInterval(long fillCacheInterval) {
            this.fillCacheInterval = fillCacheInterval;
            return this;
        }

        public Builder setTimeZone(TimeZone timeZone) {
            this.timeZone = timeZone;
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

            if (pullInterval <= 0) {
                throw new IllegalArgumentException("pullInterval should > 0");
            }

            if (fillCacheInterval <= 0) {
                throw new IllegalArgumentException("fillCacheInterval should > 0");
            }

            if (Objects.isNull(rollCycleType)) {
                throw new IllegalArgumentException("rollCycleType cannot be null");
            }

            if (Objects.isNull(timeZone)) {
                throw new IllegalArgumentException("timeZone cannot be null");
            }

            return new SimpleConsumerConfig(this);
        }

    }
}
