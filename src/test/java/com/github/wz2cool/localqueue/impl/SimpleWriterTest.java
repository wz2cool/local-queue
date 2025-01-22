package com.github.wz2cool.localqueue.impl;

import com.github.wz2cool.localqueue.model.config.SimpleWriterConfig;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class SimpleWriterTest {

    @Test
    public void testWrite() {

    }

    @Test
    public void testClose() {
        SimpleWriterConfig option = new SimpleWriterConfig.Builder()
                .setDataDir(new File("./test"))
                .setKeepDays(1)
                .build();

        SimpleWriter test;
        try (SimpleWriter simpleWriter = new SimpleWriter(option)) {
            test = simpleWriter;
        }
        assertTrue(test.isClosed());
    }
}
