package com.github.wz2cool.localqueue.impl;

import org.junit.jupiter.api.Test;

import java.io.File;

public class SimpleWriterTest {

    @Test
    public void testWrite() {

    }

    @Test
    public void testClose() throws Exception {
        SimpleWriter simpleWriter = new SimpleWriter(new File("./test"), 1);
        simpleWriter.close();
    }
}
