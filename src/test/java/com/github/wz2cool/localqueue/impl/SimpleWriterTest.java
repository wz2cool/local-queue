package com.github.wz2cool.localqueue.impl;

import com.github.wz2cool.localqueue.model.config.SimpleWriterConfig;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SimpleWriterTest {

    @Test
    public void testWriteToCache() throws IOException {
        File dir = new File("./test");
        Files.deleteIfExists(dir.toPath());
        SimpleWriterConfig option = new SimpleWriterConfig.Builder()
                .setDataDir(dir)
                .setKeepDays(1)
                .build();
        try (SimpleWriter simpleWriter = new SimpleWriter(option)) {
            // stop flush 为了让数据入到缓存中
            simpleWriter.stopFlush();
            simpleWriter.write("test1");
            simpleWriter.write("test2");
            long currentCacheCount = simpleWriter.getCurrentCacheCount();
            assertEquals(2, currentCacheCount);
        } finally {
            Files.deleteIfExists(dir.toPath());
        }
    }

    @Test
    public void testWriteToLocal() throws IOException, InterruptedException {
        File dir = new File("./test");
        FileUtils.deleteDirectory(dir);
        SimpleWriterConfig option = new SimpleWriterConfig.Builder()
                .setDataDir(dir)
                .setKeepDays(1)
                .build();
        try (SimpleWriter simpleWriter = new SimpleWriter(option)) {
            // stop flush 为了让数据入到缓存中
            simpleWriter.write("test1");
            simpleWriter.write("test2");
            // 确保数据写入
            TimeUnit.MILLISECONDS.sleep(100);
            long writePosition = simpleWriter.getWritePosition();
            System.out.println("[testWriteToLocal] write position: " + writePosition);
            assertTrue(writePosition > 0);
        } finally {
            FileUtils.deleteDirectory(dir);
        }
    }

    @Test
    public void testClose() throws IOException {
        File dir = new File("./test");
        Files.deleteIfExists(dir.toPath());
        SimpleWriterConfig option = new SimpleWriterConfig.Builder()
                .setDataDir(new File("./test"))
                .setKeepDays(1)
                .build();

        SimpleWriter test;
        try (SimpleWriter simpleWriter = new SimpleWriter(option)) {
            test = simpleWriter;
        } finally {
            Files.deleteIfExists(dir.toPath());
        }
        assertTrue(test.isClosed());
    }
}
