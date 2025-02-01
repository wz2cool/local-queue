package com.github.wz2cool.localqueue.model.config;

import com.github.wz2cool.localqueue.model.enums.RollCycleType;

import java.io.File;
import java.util.Objects;

/**
 * the config of producer
 *
 * @author frank
 */
public class SimpleProducerConfig {

    private final File dataDir;
    // -1 表示不删除
    private final int keepDays;

    private final int flushBatchSize;

    private final long flushInterval;

    private final RollCycleType rollCycleType;

    private SimpleProducerConfig(Builder builder) {
        this.dataDir = builder.dataDir;
        this.keepDays = builder.keepDays;
        this.flushBatchSize = builder.flushBatchSize;
        this.flushInterval = builder.flushInterval;
        this.rollCycleType = builder.rollCycleType;
    }

    public File getDataDir() {
        return dataDir;
    }

    public int getKeepDays() {
        return keepDays;
    }

    public int getFlushBatchSize() {
        return flushBatchSize;
    }

    public long getFlushInterval() {
        return flushInterval;
    }

    public RollCycleType getRollCycleType() {
        return rollCycleType;
    }

    public static class Builder {
        private File dataDir;
        private int keepDays = -1;
        private int flushBatchSize = 1000;
        private long flushInterval = 10;
        private RollCycleType rollCycleType = RollCycleType.HOURLY;

        public Builder setDataDir(File dataDir) {
            this.dataDir = dataDir;
            return this;
        }

        public Builder setKeepDays(int keepDays) {
            this.keepDays = keepDays;
            return this;
        }

        public Builder setFlushBatchSize(int flushBatchSize) {
            this.flushBatchSize = flushBatchSize;
            return this;
        }

        public Builder setFlushInterval(long flushInterval) {
            this.flushInterval = flushInterval;
            return this;
        }

        public Builder setRollCycleType(RollCycleType rollCycleType) {
            this.rollCycleType = rollCycleType;
            return this;
        }

        public SimpleProducerConfig build() {
            if (Objects.isNull(dataDir)) {
                throw new IllegalArgumentException("dataDir cannot be null");
            }
            if (flushBatchSize <= 0) {
                throw new IllegalArgumentException("flushBatchSize should > 0");
            }
            if (flushInterval <= 0) {
                throw new IllegalArgumentException("flushInterval should > 0");
            }

            return new SimpleProducerConfig(this);
        }
    }
}
