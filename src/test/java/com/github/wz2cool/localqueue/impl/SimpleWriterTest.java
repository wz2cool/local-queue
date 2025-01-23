package com.github.wz2cool.localqueue.impl;

import com.github.wz2cool.localqueue.model.config.SimpleWriterConfig;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class SimpleWriterTest {

    private File dir;
    private SimpleWriterConfig config;

    @BeforeEach
    public void setUp() throws IOException {
        dir = new File("./test");
        FileUtils.deleteDirectory(dir);
        config = new SimpleWriterConfig.Builder()
                .setDataDir(dir)
                .setKeepDays(1)
                .build();
    }

    @AfterEach
    public void cleanUp() throws IOException, InterruptedException {
        TimeUnit.MILLISECONDS.sleep(500);
        FileUtils.deleteDirectory(dir);
    }

    @Test
    public void testWriteToLocal() throws InterruptedException {
        try (SimpleWriter simpleWriter = new SimpleWriter(config)) {
            simpleWriter.write("init");
            // make sure data write
            TimeUnit.MILLISECONDS.sleep(50);
            long writePosition1 = simpleWriter.getLastPosition();
            simpleWriter.write("test1");
            simpleWriter.write("test2");
            // make sure data write
            TimeUnit.MILLISECONDS.sleep(50);
            long writePosition2 = simpleWriter.getLastPosition();
            long diff = writePosition2 - writePosition1;
            assertEquals(2, diff);
        }
    }

    @Test
    public void testClose() {
        SimpleWriter test;
        try (SimpleWriter simpleWriter = new SimpleWriter(config)) {
            test = simpleWriter;
        }
        assertTrue(test.isClosed());
    }

    /// region stop flush



    /// endregion
}
