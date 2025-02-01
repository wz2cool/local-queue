package com.github.wz2cool.localqueue.impl.config;

import com.github.wz2cool.localqueue.model.config.SimpleConsumerConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings("all")
public class SimpleConsumerConfigTest {

    @TempDir
    File tempDir;


    private File dataDir;
    private File positionFile;
    private String consumerId;


    @BeforeEach
    public void setUp() throws Exception {
        dataDir = tempDir;
        positionFile = new File(dataDir, "position.dat");
        consumerId = "consumerId";
    }

    @Test
    public void testBuilder() {
        // 创建一个 SimpleConsumerConfig 实例
        SimpleConsumerConfig config = new SimpleConsumerConfig.Builder()
                .setDataDir(tempDir)
                .setPositionFile(new File(tempDir, "position.dat"))
                .setConsumerId("consumer1")
                .setPullInterval(500)
                .setCacheSize(10000)
                .setFlushPositionInterval(100)
                .build();

        // 验证配置是否正确
        assertEquals(tempDir, config.getDataDir());
        assertEquals(new File(tempDir, "position.dat"), config.getPositionFile());
        assertEquals("consumer1", config.getConsumerId());
        assertEquals(500, config.getPullInterval());
        assertEquals(10000, config.getCacheSize());
        assertEquals(100, config.getFlushPositionInterval());
    }

    @Test
    public void testDataDirCannotBeNull() {
        // 测试 dataDir 为 null 时是否抛出异常
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            new SimpleConsumerConfig.Builder()
                    .setDataDir(null)
                    .setPositionFile(new File(tempDir, "position.dat"))
                    .setConsumerId("consumer1")
                    .setPullInterval(500)
                    .setCacheSize(10000)
                    .setFlushPositionInterval(100)
                    .build();
        });

        assertEquals("dataDir cannot be null", exception.getMessage());
    }

    @Test
    public void testConsumerIdCannotBeNull() {
        // 测试 consumerId 为 null 时是否抛出异常
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            new SimpleConsumerConfig.Builder()
                    .setDataDir(tempDir)
                    .setPositionFile(new File(tempDir, "position.dat"))
                    .setConsumerId(null)
                    .setPullInterval(500)
                    .setCacheSize(10000)
                    .setFlushPositionInterval(100)
                    .build();
        });

        assertEquals("consumerId cannot be null or empty", exception.getMessage());
    }

    @Test
    public void testConsumerIdCannotBeEmpty() {
        // 测试 consumerId 为空字符串时是否抛出异常
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            new SimpleConsumerConfig.Builder()
                    .setDataDir(tempDir)
                    .setPositionFile(new File(tempDir, "position.dat"))
                    .setConsumerId("")
                    .setPullInterval(500)
                    .setCacheSize(10000)
                    .setFlushPositionInterval(100)
                    .build();
        });

        assertEquals("consumerId cannot be null or empty", exception.getMessage());
    }

    @Test
    public void testCacheSizeShouldBeGreaterThanZero() {
        // 测试 cacheSize 小于等于 0 时是否抛出异常
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            new SimpleConsumerConfig.Builder()
                    .setDataDir(tempDir)
                    .setPositionFile(new File(tempDir, "position.dat"))
                    .setConsumerId("consumer1")
                    .setPullInterval(500)
                    .setCacheSize(0)
                    .setFlushPositionInterval(100)
                    .build();
        });

        assertEquals("cacheSize should > 0", exception.getMessage());

        exception = assertThrows(IllegalArgumentException.class, () -> {
            new SimpleConsumerConfig.Builder()
                    .setDataDir(tempDir)
                    .setPositionFile(new File(tempDir, "position.dat"))
                    .setConsumerId("consumer1")
                    .setPullInterval(500)
                    .setCacheSize(-100)
                    .setFlushPositionInterval(100)
                    .build();
        });

        assertEquals("cacheSize should > 0", exception.getMessage());
    }

    @Test
    public void testFlushPositionIntervalShouldBeGreaterThanZero() {
        // 测试 flushPositionInterval 小于等于 0 时是否抛出异常
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            new SimpleConsumerConfig.Builder()
                    .setDataDir(tempDir)
                    .setPositionFile(new File(tempDir, "position.dat"))
                    .setConsumerId("consumer1")
                    .setPullInterval(500)
                    .setCacheSize(10000)
                    .setFlushPositionInterval(0)
                    .build();
        });

        assertEquals("flushPositionInterval should > 0", exception.getMessage());

        exception = assertThrows(IllegalArgumentException.class, () -> {
            new SimpleConsumerConfig.Builder()
                    .setDataDir(tempDir)
                    .setPositionFile(new File(tempDir, "position.dat"))
                    .setConsumerId("consumer1")
                    .setPullInterval(500)
                    .setCacheSize(10000)
                    .setFlushPositionInterval(-100)
                    .build();
        });

        assertEquals("flushPositionInterval should > 0", exception.getMessage());
    }

    @Test
    public void testDefaultPositionFile() {
        // 测试默认的 positionFile 是否为 dataDir 下的 "position.dat"
        SimpleConsumerConfig config = new SimpleConsumerConfig.Builder()
                .setDataDir(tempDir)
                .setConsumerId("consumer1")
                .setPullInterval(500)
                .setCacheSize(10000)
                .setFlushPositionInterval(100)
                .build();

        assertEquals(new File(tempDir, "position.dat"), config.getPositionFile());
    }

    @Test
    public void testDefaultPullInterval() {
        // 测试默认的 pullInterval 是否为 500
        SimpleConsumerConfig config = new SimpleConsumerConfig.Builder()
                .setDataDir(tempDir)
                .setPositionFile(new File(tempDir, "position.dat"))
                .setConsumerId("consumer1")
                .setCacheSize(10000)
                .setFlushPositionInterval(100)
                .build();

        assertEquals(10, config.getPullInterval());
    }

    @Test
    public void testDefaultCacheSize() {
        // 测试默认的 cacheSize 是否为 10000
        SimpleConsumerConfig config = new SimpleConsumerConfig.Builder()
                .setDataDir(tempDir)
                .setPositionFile(new File(tempDir, "position.dat"))
                .setConsumerId("consumer1")
                .setPullInterval(500)
                .setFlushPositionInterval(100)
                .build();

        assertEquals(10000, config.getCacheSize());
    }

    @Test
    public void testDefaultFlushPositionInterval() {
        // 测试默认的 flushPositionInterval 是否为 100
        SimpleConsumerConfig config = new SimpleConsumerConfig.Builder()
                .setDataDir(tempDir)
                .setPositionFile(new File(tempDir, "position.dat"))
                .setConsumerId("consumer1")
                .setPullInterval(500)
                .setCacheSize(10000)
                .build();

        assertEquals(100, config.getFlushPositionInterval());
    }

    @Test
    public void build_ValidConfig_ShouldCreateConfig() {
        SimpleConsumerConfig config = new SimpleConsumerConfig.Builder()
                .setDataDir(dataDir)
                .setPositionFile(positionFile)
                .setConsumerId(consumerId)
                .build();

        assertEquals(dataDir, config.getDataDir());
        assertEquals(positionFile, config.getPositionFile());
        assertEquals(consumerId, config.getConsumerId());
    }

    @Test
    public void build_NullDataDir_ShouldThrowException() {
        assertThrows(IllegalArgumentException.class, () -> {
            new SimpleConsumerConfig.Builder()
                    .setDataDir(null)
                    .setPositionFile(positionFile)
                    .setConsumerId(consumerId)
                    .build();
        });
    }

    @Test
    public void build_NullConsumerId_ShouldThrowException() {
        assertThrows(IllegalArgumentException.class, () -> {
            new SimpleConsumerConfig.Builder()
                    .setDataDir(dataDir)
                    .setPositionFile(positionFile)
                    .setConsumerId(null)
                    .build();
        });
    }

    @Test
    public void build_EmptyConsumerId_ShouldThrowException() {
        assertThrows(IllegalArgumentException.class, () -> {
            new SimpleConsumerConfig.Builder()
                    .setDataDir(dataDir)
                    .setPositionFile(positionFile)
                    .setConsumerId("")
                    .build();
        });
    }

    @Test
    public void build_NegativeCacheSize_ShouldThrowException() {
        assertThrows(IllegalArgumentException.class, () -> {
            new SimpleConsumerConfig.Builder()
                    .setDataDir(dataDir)
                    .setPositionFile(positionFile)
                    .setConsumerId(consumerId)
                    .setCacheSize(-1)
                    .build();
        });
    }

    @Test
    public void build_ZeroFlushPositionInterval_ShouldThrowException() {
        assertThrows(IllegalArgumentException.class, () -> {
            new SimpleConsumerConfig.Builder()
                    .setDataDir(dataDir)
                    .setPositionFile(positionFile)
                    .setConsumerId(consumerId)
                    .setFlushPositionInterval(0)
                    .build();
        });
    }

    @Test
    public void build_NullPositionFile_ShouldUseDefault() {
        SimpleConsumerConfig config = new SimpleConsumerConfig.Builder()
                .setDataDir(dataDir)
                .setConsumerId(consumerId)
                .build();

        assertEquals(new File(dataDir, "position.dat"), config.getPositionFile());
    }

    @Test
    public void build_NullConsumeFromWhere_ShouldThrowException() {
        assertThrows(IllegalArgumentException.class, () -> {
            new SimpleConsumerConfig.Builder()
                    .setDataDir(dataDir)
                    .setPositionFile(positionFile)
                    .setConsumerId(consumerId)
                    .setConsumeFromWhere(null)
                    .build();
        });
    }

    @Test
    public void build_NegativePullInterval_ShouldThrowException() {
        assertThrows(IllegalArgumentException.class, () -> {
            new SimpleConsumerConfig.Builder()
                    .setDataDir(dataDir)
                    .setPositionFile(positionFile)
                    .setConsumerId(consumerId)
                    .setPullInterval(-1)
                    .build();
        });
    }

    @Test
    public void build_NegativeFillCacheInterval_ShouldThrowException() {
        assertThrows(IllegalArgumentException.class, () -> {
            new SimpleConsumerConfig.Builder()
                    .setDataDir(dataDir)
                    .setPositionFile(positionFile)
                    .setConsumerId(consumerId)
                    .setFillCacheInterval(-1)
                    .build();
        });
    }
}
