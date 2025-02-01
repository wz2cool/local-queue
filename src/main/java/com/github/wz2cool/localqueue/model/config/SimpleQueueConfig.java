package com.github.wz2cool.localqueue.model.config;

import com.github.wz2cool.localqueue.model.enums.RollCycleType;

import java.io.File;

public class SimpleQueueConfig {

    private final File dataDir;
    // -1 表示不删除
    private final int keepDays;

    private final RollCycleType rollCycleType;

    private SimpleQueueConfig(final Builder builder) {
        this.dataDir = builder.dataDir;
        this.keepDays = builder.keepDays;
        this.rollCycleType = builder.rollCycleType;
    }

    public File getDataDir() {
        return dataDir;
    }

    public int getKeepDays() {
        return keepDays;
    }

    public RollCycleType getRollCycleType() {
        return rollCycleType;
    }

    public static class Builder {
        private File dataDir;
        private int keepDays;
        private RollCycleType rollCycleType = RollCycleType.HOURLY;

        public Builder setDataDir(File dataDir) {
            this.dataDir = dataDir;
            return this;
        }

        public Builder setKeepDays(int keepDays) {
            this.keepDays = keepDays;
            return this;
        }

        public Builder setRollCycleType(RollCycleType rollCycleType) {
            this.rollCycleType = rollCycleType;
            return this;
        }

        public SimpleQueueConfig build() {
            return new SimpleQueueConfig(this);
        }
    }
}
