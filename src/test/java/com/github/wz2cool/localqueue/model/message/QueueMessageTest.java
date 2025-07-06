package com.github.wz2cool.localqueue.model.message;

import com.github.wz2cool.localqueue.model.message.internal.HeaderMessage;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * QueueMessage 测试用例
 *
 * @author frank
 */
public class QueueMessageTest {

    @Test
    public void testConstructorWithAllParameters() {
        Map<String, String> headers = new HashMap<>();
        headers.put("key1", "value1");
        HeaderMessage headerMessage = new HeaderMessage(headers);
        
        QueueMessage queueMessage = new QueueMessage(
                "testTag",
                "testMessageKey",
                1,
                100L,
                "test content",
                System.currentTimeMillis(),
                headerMessage
        );
        
        assertNotNull(queueMessage);
        assertEquals("testTag", queueMessage.getTag());
        assertEquals("testMessageKey", queueMessage.getMessageKey());
        assertEquals(1, queueMessage.getPositionVersion());
        assertEquals(100L, queueMessage.getPosition());
        assertEquals("test content", queueMessage.getContent());
        assertTrue(queueMessage.getWriteTime() > 0);
        assertNotNull(queueMessage.getHeaderKeys());
    }

    @Test
    public void testConstructorWithNullValues() {
        QueueMessage queueMessage = new QueueMessage(
                null,
                null,
                0,
                0L,
                null,
                0L,
                null
        );
        
        assertNotNull(queueMessage);
        assertNull(queueMessage.getTag());
        assertNull(queueMessage.getMessageKey());
        assertEquals(0, queueMessage.getPositionVersion());
        assertEquals(0L, queueMessage.getPosition());
        assertNull(queueMessage.getContent());
        assertEquals(0L, queueMessage.getWriteTime());
        assertTrue(queueMessage.getHeaderKeys().isEmpty());
    }

    @Test
    public void testGetPosition() {
        QueueMessage queueMessage = new QueueMessage(
                "testTag",
                "testMessageKey",
                1,
                12345L,
                "test content",
                System.currentTimeMillis(),
                null
        );
        
        assertEquals(12345L, queueMessage.getPosition());
    }

    @Test
    public void testGetContent() {
        String testContent = "This is a test content";
        QueueMessage queueMessage = new QueueMessage(
                "testTag",
                "testMessageKey",
                1,
                100L,
                testContent,
                System.currentTimeMillis(),
                null
        );
        
        assertEquals(testContent, queueMessage.getContent());
    }

    @Test
    public void testGetPositionVersion() {
        QueueMessage queueMessage = new QueueMessage(
                "testTag",
                "testMessageKey",
                999,
                100L,
                "test content",
                System.currentTimeMillis(),
                null
        );
        
        assertEquals(999, queueMessage.getPositionVersion());
    }

    @Test
    public void testGetWriteTime() {
        long currentTime = System.currentTimeMillis();
        QueueMessage queueMessage = new QueueMessage(
                "testTag",
                "testMessageKey",
                1,
                100L,
                "test content",
                currentTime,
                null
        );
        
        assertEquals(currentTime, queueMessage.getWriteTime());
    }

    @Test
    public void testGetMessageKey() {
        String messageKey = "unique-message-key-12345";
        QueueMessage queueMessage = new QueueMessage(
                "testTag",
                messageKey,
                1,
                100L,
                "test content",
                System.currentTimeMillis(),
                null
        );
        
        assertEquals(messageKey, queueMessage.getMessageKey());
    }

    @Test
    public void testGetTag() {
        String tag = "important-tag";
        QueueMessage queueMessage = new QueueMessage(
                tag,
                "testMessageKey",
                1,
                100L,
                "test content",
                System.currentTimeMillis(),
                null
        );
        
        assertEquals(tag, queueMessage.getTag());
    }

    @Test
    public void testGetHeaderValueWithNullHeaderMessage() {
        QueueMessage queueMessage = new QueueMessage(
                "testTag",
                "testMessageKey",
                1,
                100L,
                "test content",
                System.currentTimeMillis(),
                null
        );
        
        Optional<String> result = queueMessage.getHeaderValue("anyKey");
        assertFalse(result.isPresent());
    }

    @Test
    public void testGetHeaderValueWithHeaders() {
        Map<String, String> headers = new HashMap<>();
        headers.put("contentType", "application/json");
        headers.put("messageId", "12345");
        HeaderMessage headerMessage = new HeaderMessage(headers);
        
        QueueMessage queueMessage = new QueueMessage(
                "testTag",
                "testMessageKey",
                1,
                100L,
                "test content",
                System.currentTimeMillis(),
                headerMessage
        );
        
        Optional<String> result = queueMessage.getHeaderValue("contentType");
        assertTrue(result.isPresent());
        assertEquals("application/json", result.get());
        
        Optional<String> result2 = queueMessage.getHeaderValue("nonExisting");
        assertFalse(result2.isPresent());
    }

    @Test
    public void testGetHeaderKeysWithNullHeaderMessage() {
        QueueMessage queueMessage = new QueueMessage(
                "testTag",
                "testMessageKey",
                1,
                100L,
                "test content",
                System.currentTimeMillis(),
                null
        );
        
        Set<String> keys = queueMessage.getHeaderKeys();
        assertNotNull(keys);
        assertTrue(keys.isEmpty());
    }

    @Test
    public void testGetHeaderKeysWithHeaders() {
        Map<String, String> headers = new HashMap<>();
        headers.put("key1", "value1");
        headers.put("key2", "value2");
        headers.put("key3", "value3");
        HeaderMessage headerMessage = new HeaderMessage(headers);
        
        QueueMessage queueMessage = new QueueMessage(
                "testTag",
                "testMessageKey",
                1,
                100L,
                "test content",
                System.currentTimeMillis(),
                headerMessage
        );
        
        Set<String> keys = queueMessage.getHeaderKeys();
        assertNotNull(keys);
        assertEquals(3, keys.size());
        assertTrue(keys.contains("key1"));
        assertTrue(keys.contains("key2"));
        assertTrue(keys.contains("key3"));
    }

    @Test
    public void testCompleteMessageWithAllFeatures() {
        Map<String, String> headers = new HashMap<>();
        headers.put("contentType", "application/json");
        headers.put("messageId", "msg-12345");
        headers.put("priority", "high");
        HeaderMessage headerMessage = new HeaderMessage(headers);
        
        long currentTime = System.currentTimeMillis();
        QueueMessage queueMessage = new QueueMessage(
                "important",
                "unique-key-67890",
                5,
                999L,
                "{\"data\": \"test json content\"}",
                currentTime,
                headerMessage
        );
        
        // 测试所有 getter 方法
        assertEquals("important", queueMessage.getTag());
        assertEquals("unique-key-67890", queueMessage.getMessageKey());
        assertEquals(5, queueMessage.getPositionVersion());
        assertEquals(999L, queueMessage.getPosition());
        assertEquals("{\"data\": \"test json content\"}", queueMessage.getContent());
        assertEquals(currentTime, queueMessage.getWriteTime());
        
        // 测试 header 功能
        assertEquals(3, queueMessage.getHeaderKeys().size());
        assertEquals("application/json", queueMessage.getHeaderValue("contentType").orElse(null));
        assertEquals("msg-12345", queueMessage.getHeaderValue("messageId").orElse(null));
        assertEquals("high", queueMessage.getHeaderValue("priority").orElse(null));
        assertFalse(queueMessage.getHeaderValue("nonExisting").isPresent());
    }

    @Test
    public void testNegativeValues() {
        QueueMessage queueMessage = new QueueMessage(
                "testTag",
                "testMessageKey",
                -1,
                -100L,
                "test content",
                -1000L,
                null
        );
        
        assertEquals(-1, queueMessage.getPositionVersion());
        assertEquals(-100L, queueMessage.getPosition());
        assertEquals(-1000L, queueMessage.getWriteTime());
    }

    @Test
    public void testEmptyStringValues() {
        QueueMessage queueMessage = new QueueMessage(
                "",
                "",
                0,
                0L,
                "",
                0L,
                null
        );
        
        assertEquals("", queueMessage.getTag());
        assertEquals("", queueMessage.getMessageKey());
        assertEquals("", queueMessage.getContent());
    }

    @Test
    public void testMaxValues() {
        QueueMessage queueMessage = new QueueMessage(
                "testTag",
                "testMessageKey",
                Integer.MAX_VALUE,
                Long.MAX_VALUE,
                "test content",
                Long.MAX_VALUE,
                null
        );
        
        assertEquals(Integer.MAX_VALUE, queueMessage.getPositionVersion());
        assertEquals(Long.MAX_VALUE, queueMessage.getPosition());
        assertEquals(Long.MAX_VALUE, queueMessage.getWriteTime());
    }
}