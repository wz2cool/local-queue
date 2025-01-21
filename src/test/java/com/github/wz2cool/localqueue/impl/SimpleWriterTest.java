package com.github.wz2cool.localqueue.impl;

import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class SimpleWriterTest {

    @Test
    public void testWrite() {

    }

    @Test
    public void testClose() {
        SimpleWriter test;
        try (SimpleWriter simpleWriter = new SimpleWriter(new File("./test"), 1)) {
            test = simpleWriter;
        }
        assertTrue(test.isClosed());
    }
}
