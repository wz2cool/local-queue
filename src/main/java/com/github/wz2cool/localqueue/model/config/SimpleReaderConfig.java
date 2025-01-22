package com.github.wz2cool.localqueue.model.config;

import java.io.File;
import java.util.Objects;

public class SimpleReaderConfig {

    private final File dataDir;

    private final File positionFile;

    private final String readerKey;

    private final long pullInterval;

    private SimpleReaderConfig(final Builder builder) {
        this.dataDir = builder.dataDir;
        this.positionFile = builder.positionFile;
        this.readerKey = builder.readerKey;
        this.pullInterval = builder.pullInterval;
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

    public static class Builder {

        private File dataDir;

        private File positionFile;

        private String readerKey;

        private long pullInterval = 500;

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

        public SimpleReaderConfig build() {
            if (Objects.isNull(dataDir)) {
                throw new IllegalArgumentException("dataDir cannot be null");
            }

            if (Objects.isNull(readerKey) || readerKey.isEmpty()) {
                // 如果没有就给默认
                throw new IllegalArgumentException("readerKey cannot be null or empty");
            }

            if (Objects.isNull(positionFile)) {
                // 如果没有就给默认
                this.positionFile = new File(dataDir, "position.dat");
            }

            return new SimpleReaderConfig(this);
        }

    }
}
