package com.github.wz2cool.localqueue.model.config;

import java.io.File;

public class SimpleQueueConfig {

    private final File dataDir;
    // -1 表示不删除
    private final int keepDays;

    private SimpleQueueConfig(final Builder builder) {
        this.dataDir = builder.dataDir;
        this.keepDays = builder.keepDays;
    }

    public File getDataDir() {
        return dataDir;
    }

    public int getKeepDays() {
        return keepDays;
    }

    public static class Builder {
        private File dataDir;
        private int keepDays;

        public Builder setDataDir(File dataDir) {
            this.dataDir = dataDir;
            return this;
        }

        public Builder setKeepDays(int keepDays) {
            this.keepDays = keepDays;
            return this;
        }

        public SimpleQueueConfig build() {
            return new SimpleQueueConfig(this);
        }
    }
}
