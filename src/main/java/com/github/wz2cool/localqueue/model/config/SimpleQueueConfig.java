package com.github.wz2cool.localqueue.model.config;

import com.github.wz2cool.localqueue.model.enums.RollCycleType;

import java.io.File;
import java.util.Objects;
import java.util.TimeZone;

public class SimpleQueueConfig {

    private final File dataDir;
    // -1 表示不删除
    private final int keepDays;

    private final RollCycleType rollCycleType;

    private final TimeZone timeZone;

    private SimpleQueueConfig(final Builder builder) {
        this.dataDir = builder.dataDir;
        this.keepDays = builder.keepDays;
        this.rollCycleType = builder.rollCycleType;
        this.timeZone = builder.timeZone;
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

    public TimeZone getTimeZone() {
        return timeZone;
    }

    public static class Builder {
        private File dataDir;
        private int keepDays;
        private RollCycleType rollCycleType = RollCycleType.HOURLY;
        private TimeZone timeZone = TimeZone.getDefault();

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

        public Builder setTimeZone(TimeZone timeZone) {
            this.timeZone = timeZone;
            return this;
        }

        public SimpleQueueConfig build() {
            if (Objects.isNull(dataDir)) {
                throw new IllegalArgumentException("dataDir cannot be null");
            }
            if (keepDays < -1) {
                throw new IllegalArgumentException("keepDays should >= -1");
            }
            if (Objects.isNull(rollCycleType)) {
                throw new IllegalArgumentException("rollCycleType cannot be null");
            }
            if (Objects.isNull(timeZone)) {
                throw new IllegalArgumentException("timeZone cannot be null");
            }

            return new SimpleQueueConfig(this);
        }
    }
}
