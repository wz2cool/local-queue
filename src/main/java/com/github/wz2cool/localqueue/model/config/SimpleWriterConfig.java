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

    private SimpleWriterConfig(Builder builder) {
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
        private int keepDays = -1;

        public Builder setDataDir(File dataDir) {
            this.dataDir = dataDir;
            return this;
        }

        public Builder setKeepDays(int keepDays) {
            this.keepDays = keepDays;
            return this;
        }

        public SimpleWriterConfig build() {
            if (Objects.isNull(dataDir)) {
                throw new IllegalArgumentException("dataDir cannot be null");
            }
            return new SimpleWriterConfig(this);
        }
    }
}
