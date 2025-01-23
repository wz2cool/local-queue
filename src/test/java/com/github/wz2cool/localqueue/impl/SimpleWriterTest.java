package com.github.wz2cool.localqueue.impl;

import com.github.wz2cool.localqueue.model.config.SimpleWriterConfig;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SimpleWriterTest {

    @Test
    public void testWrite() {
        SimpleWriterConfig option = new SimpleWriterConfig.Builder()
                .setDataDir(new File("./test"))
                .setKeepDays(1)
                .build();
        try (SimpleWriter simpleWriter = new SimpleWriter(option)) {
            // stop flush 为了让数据入到缓存中
            simpleWriter.stopFlush();
            simpleWriter.write("test1");
            simpleWriter.write("test2");
            long currentCacheCount = simpleWriter.getCurrentCacheCount();
            assertEquals(2, currentCacheCount);
        }
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
