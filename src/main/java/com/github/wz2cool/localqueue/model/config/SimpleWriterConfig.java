package com.github.wz2cool.localqueue.model.config;

import java.io.File;
import java.util.Objects;

/**
 * 简单写入器参数
 *
 * @author frank
 */
public class SimpleWriterConfig {

    private final File dataDir;
    // -1 表示不删除
    private final int keepDays;

    private final int flushBatchSize;

    private SimpleWriterConfig(Builder builder) {
        this.dataDir = builder.dataDir;
        this.keepDays = builder.keepDays;
        this.flushBatchSize = builder.flushBatchSize;
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

    public static class Builder {
        private File dataDir;
        private int keepDays = -1;
        private int flushBatchSize = 1000;

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

        public SimpleWriterConfig build() {
            if (Objects.isNull(dataDir)) {
                throw new IllegalArgumentException("dataDir cannot be null");
            }
            if (flushBatchSize <= 0) {
                throw new IllegalArgumentException("flushBatchSize should > 0");
            }
            return new SimpleWriterConfig(this);
        }
    }
}
