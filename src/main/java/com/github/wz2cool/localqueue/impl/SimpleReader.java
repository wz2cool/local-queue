package com.github.wz2cool.localqueue.impl;

import com.github.wz2cool.localqueue.IReader;
import com.github.wz2cool.localqueue.model.option.SimpleReaderConfig;

public class SimpleReader implements IReader {

    private final SimpleReaderConfig config;

    public SimpleReader(final SimpleReaderConfig config) {
        this.config = config;
    }
}
