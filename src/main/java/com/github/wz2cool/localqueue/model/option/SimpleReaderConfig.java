package com.github.wz2cool.localqueue.model.option;

import java.io.File;
import java.util.Objects;

public class SimpleReaderConfig {

    private final File dataDir;

    private final File positionFile;

    private final String positionKey;

    private SimpleReaderConfig(final Builder builder) {
        this.dataDir = builder.dataDir;
        this.positionFile = builder.positionFile;
        this.positionKey = builder.positionKey;
    }

    public static class Builder {

        private File dataDir;

        private File positionFile;

        private String positionKey;

        public Builder setDataDir(File dataDir) {
            this.dataDir = dataDir;
            return this;
        }

        public Builder setPositionFile(File positionFile) {
            this.positionFile = positionFile;
            return this;
        }

        public Builder setPositionKey(String positionKey) {
            this.positionKey = positionKey;
            return this;
        }

        public SimpleReaderConfig build() {
            if (Objects.isNull(dataDir)) {
                throw new IllegalArgumentException("dataDir cannot be null");
            }

            if (Objects.isNull(positionKey) || positionKey.isEmpty()) {
                // 如果没有就给默认
                throw new IllegalArgumentException("positionKey cannot be null or empty");
            }

            if (Objects.isNull(positionFile)) {
                // 如果没有就给默认
                this.positionFile = new File(dataDir, "position.dat");
            }

            return new SimpleReaderConfig(this);
        }

    }
}
