package com.github.wz2cool.localqueue.impl.config;

import com.github.wz2cool.localqueue.model.config.SimpleProducerConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings("all")
public class SimpleProducerConfigTest {

    @TempDir
    File tempDir;

    @Test
    public void testBuilder() {
        // 创建一个 SimpleProducerConfig 实例
        SimpleProducerConfig config = new SimpleProducerConfig.Builder()
                .setDataDir(tempDir)
                .setKeepDays(7)
                .setFlushBatchSize(1000)
                .setFlushInterval(10)
                .build();

        // 验证配置是否正确
        assertEquals(tempDir, config.getDataDir());
        assertEquals(7, config.getKeepDays());
        assertEquals(1000, config.getFlushBatchSize());
        assertEquals(10, config.getFlushInterval());
    }

    @Test
    public void testDataDirCannotBeNull() {
        // 测试 dataDir 为 null 时是否抛出异常
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            new SimpleProducerConfig.Builder()
                    .setDataDir(null)
                    .setKeepDays(7)
                    .setFlushBatchSize(1000)
                    .build();
        });

        assertEquals("dataDir cannot be null", exception.getMessage());
    }

    @Test
    public void testFlushBatchSizeShouldBeGreaterThanZero() {
        // 测试 flushBatchSize 小于等于 0 时是否抛出异常
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            new SimpleProducerConfig.Builder()
                    .setDataDir(tempDir)
                    .setKeepDays(7)
                    .setFlushBatchSize(0)
                    .build();
        });

        assertEquals("flushBatchSize should > 0", exception.getMessage());

        exception = assertThrows(IllegalArgumentException.class, () -> {
            new SimpleProducerConfig.Builder()
                    .setDataDir(tempDir)
                    .setKeepDays(7)
                    .setFlushBatchSize(-100)
                    .build();
        });

        assertEquals("flushBatchSize should > 0", exception.getMessage());
    }

    @Test
    public void testDefaultKeepDays() {
        // 测试默认的 keepDays 是否为 -1
        SimpleProducerConfig config = new SimpleProducerConfig.Builder()
                .setDataDir(tempDir)
                .setFlushBatchSize(1000)
                .build();

        assertEquals(-1, config.getKeepDays());
    }

    @Test
    public void testDefaultFlushBatchSize() {
        // 测试默认的 flushBatchSize 是否为 1000
        SimpleProducerConfig config = new SimpleProducerConfig.Builder()
                .setDataDir(tempDir)
                .setKeepDays(7)
                .build();

        assertEquals(1000, config.getFlushBatchSize());
    }
}
