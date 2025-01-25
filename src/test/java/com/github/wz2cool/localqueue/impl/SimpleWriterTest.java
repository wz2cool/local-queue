package com.github.wz2cool.localqueue.impl;

import com.github.wz2cool.localqueue.model.config.SimpleWriterConfig;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("all")
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
        FileUtils.deleteDirectory(dir);
    }

    @Test
    public void testOfferToLocal() throws InterruptedException {
        try (SimpleWriter simpleWriter = new SimpleWriter(config)) {
            simpleWriter.offer("init");
            // make sure data write
            TimeUnit.MILLISECONDS.sleep(100);
            long writePosition1 = simpleWriter.getLastPosition();
            simpleWriter.offer("test1");
            simpleWriter.offer("test2");
            // make sure data write
            TimeUnit.MILLISECONDS.sleep(100);
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

    /// region cleanUpOldFile
    @Test
    public void cleanUpOldFile_FileOlderThanKeepDate_FileDeleted() throws Exception {
        File oldFile = new File(dir, "20230101F.cq4");
        FileUtils.createParentDirectories(oldFile);
        oldFile.createNewFile();
        LocalDate keepDate = LocalDate.of(2023, 1, 2);
        try (SimpleWriter simpleWriter = new SimpleWriter(config)) {
            invokePrivateMethod(simpleWriter, "cleanUpOldFile", new Class[]{File.class, LocalDate.class}, oldFile, keepDate);
        }
        assertFalse(oldFile.exists(), "Old file should be deleted");
    }

    @Test
    public void cleanUpOldFile_FileNewerThanKeepDate_FileNotDeleted() throws Exception {
        File newFile = new File(dir, "20230102F.cq4");
        FileUtils.createParentDirectories(newFile);
        newFile.createNewFile();
        LocalDate keepDate = LocalDate.of(2023, 1, 1);
        try (SimpleWriter simpleWriter = new SimpleWriter(config)) {
            invokePrivateMethod(simpleWriter, "cleanUpOldFile", new Class[]{File.class, LocalDate.class}, newFile, keepDate);
        }
        assertTrue(newFile.exists(), "New file should not be deleted");
    }

    @Test
    public void cleanUpOldFile_FileEqualToDate_FileNotDeleted() throws Exception {
        File file = new File(dir, "20230101F.cq4");
        FileUtils.createParentDirectories(file);
        file.createNewFile();
        LocalDate keepDate = LocalDate.of(2023, 1, 1);
        try (SimpleWriter simpleWriter = new SimpleWriter(config)) {
            invokePrivateMethod(simpleWriter, "cleanUpOldFile", new Class[]{File.class, LocalDate.class}, file, keepDate);
        }
        assertTrue(file.exists(), "File with date equal to keepDate should not be deleted");
    }

    /// endregion

    /// region cleanUpOldFiles

    @Test
    public void cleanUpOldFiles_KeepDaysMinusOne_NoFilesDeleted() throws Exception {
        File oldFile = new File(dir, "20230101F.cq4");
        FileUtils.createParentDirectories(oldFile);
        oldFile.createNewFile();
        try (SimpleWriter simpleWriter = new SimpleWriter(config)) {
            invokePrivateMethod(simpleWriter, "cleanUpOldFiles", new Class[]{int.class}, -1);
        }
        assertTrue(oldFile.exists(), "File should not be deleted when keepDays is -1");
    }

    @Test
    public void cleanUpOldFiles_NoFilesInDirectory_NoFilesDeleted() throws Exception {
        try (SimpleWriter simpleWriter = new SimpleWriter(config)) {
            invokePrivateMethod(simpleWriter, "cleanUpOldFiles", new Class[]{int.class}, 1);
        }
        // No files should be deleted as there are no files in the directory, and should see No file found log.

    }

    @Test
    public void cleanUpOldFiles_FileNewerThanKeepDate_FileNotDeleted() throws Exception {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        LocalDate keepDate = LocalDate.now();
        String format = formatter.format(keepDate);
        String fileName = format + "F.cq4";

        File newFile = new File(dir, fileName);
        FileUtils.createParentDirectories(newFile);
        newFile.createNewFile();
        try (SimpleWriter simpleWriter = new SimpleWriter(config)) {
            invokePrivateMethod(simpleWriter, "cleanUpOldFiles", new Class[]{int.class}, 1);
        }
        assertTrue(newFile.exists(), "New file should not be deleted");
    }


    /// endregion

    private void invokePrivateMethod(Object object, String methodName, Class<?>[] parameterTypes, Object... parameters) throws Exception {
        Method method = object.getClass().getDeclaredMethod(methodName, parameterTypes);
        method.setAccessible(true);
        method.invoke(object, parameters);
    }
}
