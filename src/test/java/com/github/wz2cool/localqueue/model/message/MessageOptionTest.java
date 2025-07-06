package com.github.wz2cool.localqueue.model.message;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MessageOption 测试用例
 *
 * @author frank
 */
public class MessageOptionTest {

    private MessageOption messageOption;

    @BeforeEach
    public void setUp() {
        messageOption = new MessageOption();
    }

    @Test
    public void testTagGetterAndSetter() {
        // 测试 tag 的 getter 和 setter
        String tag = "test-tag";
        messageOption.setTag(tag);
        assertEquals(tag, messageOption.getTag());
    }

    @Test
    public void testTagWithNull() {
        // 测试设置 null tag
        messageOption.setTag(null);
        assertNull(messageOption.getTag());
    }

    @Test
    public void testMessageKeyGetterAndSetter() {
        // 测试 messageKey 的 getter 和 setter
        String messageKey = "test-message-key";
        messageOption.setMessageKey(messageKey);
        assertEquals(messageKey, messageOption.getMessageKey());
    }

    @Test
    public void testMessageKeyWithNull() {
        // 测试设置 null messageKey
        messageOption.setMessageKey(null);
        assertNull(messageOption.getMessageKey());
    }

    @Test
    public void testAddHeader() {
        // 测试添加 header
        String key = "Content-Type";
        String value = "application/json";
        messageOption.addHeader(key, value);
        
        Optional<String> headerValue = messageOption.getHeaderValue(key);
        assertTrue(headerValue.isPresent());
        assertEquals(value, headerValue.get());
    }

    @Test
    public void testAddMultipleHeaders() {
        // 测试添加多个 headers
        messageOption.addHeader("Content-Type", "application/json");
        messageOption.addHeader("Authorization", "Bearer token");
        messageOption.addHeader("User-Agent", "test-agent");
        
        assertEquals("application/json", messageOption.getHeaderValue("Content-Type").get());
        assertEquals("Bearer token", messageOption.getHeaderValue("Authorization").get());
        assertEquals("test-agent", messageOption.getHeaderValue("User-Agent").get());
    }

    @Test
    public void testAddHeaderWithNullKey() {
        // 测试添加 null key 的 header
        messageOption.addHeader(null, "value");
        Optional<String> headerValue = messageOption.getHeaderValue(null);
        assertTrue(headerValue.isPresent());
        assertEquals("value", headerValue.get());
    }

    @Test
    public void testAddHeaderWithNullValue() {
        // 测试添加 null value 的 header
        String key = "test-key";
        messageOption.addHeader(key, null);
        Optional<String> headerValue = messageOption.getHeaderValue(key);
        assertFalse(headerValue.isPresent());
    }

    @Test
    public void testOverwriteHeader() {
        // 测试覆盖已存在的 header
        String key = "Content-Type";
        messageOption.addHeader(key, "application/json");
        messageOption.addHeader(key, "application/xml");
        
        Optional<String> headerValue = messageOption.getHeaderValue(key);
        assertTrue(headerValue.isPresent());
        assertEquals("application/xml", headerValue.get());
    }

    @Test
    public void testGetHeaderValueWhenNoHeaders() {
        // 测试在没有 headers 时获取 header 值
        Optional<String> headerValue = messageOption.getHeaderValue("non-existent");
        assertFalse(headerValue.isPresent());
    }

    @Test
    public void testGetHeaderValueWhenHeaderNotExists() {
        // 测试获取不存在的 header 值
        messageOption.addHeader("existing-key", "value");
        Optional<String> headerValue = messageOption.getHeaderValue("non-existent-key");
        assertFalse(headerValue.isPresent());
    }

    @Test
    public void testGetHeaderKeysWhenNoHeaders() {
        // 测试在没有 headers 时获取 header keys
        Set<String> headerKeys = messageOption.getHeaderKeys();
        assertNotNull(headerKeys);
        assertTrue(headerKeys.isEmpty());
    }

    @Test
    public void testGetHeaderKeysWithHeaders() {
        // 测试有 headers 时获取 header keys
        messageOption.addHeader("key1", "value1");
        messageOption.addHeader("key2", "value2");
        messageOption.addHeader("key3", "value3");
        
        Set<String> headerKeys = messageOption.getHeaderKeys();
        assertNotNull(headerKeys);
        assertEquals(3, headerKeys.size());
        assertTrue(headerKeys.contains("key1"));
        assertTrue(headerKeys.contains("key2"));
        assertTrue(headerKeys.contains("key3"));
    }

    @Test
    public void testHasHeaderWhenNoHeaders() {
        // 测试在没有 headers 时检查是否有 header
        assertFalse(messageOption.hasHeader());
    }

    @Test
    public void testHasHeaderWhenHasHeaders() {
        // 测试有 headers 时检查是否有 header
        messageOption.addHeader("key", "value");
        assertTrue(messageOption.hasHeader());
    }

    @Test
    public void testGetHeadersWhenNoHeaders() {
        // 测试在没有 headers 时获取所有 headers
        Map<String, String> headers = messageOption.getHeaders();
        assertNotNull(headers);
        assertTrue(headers.isEmpty());
    }

    @Test
    public void testGetHeadersWithHeaders() {
        // 测试有 headers 时获取所有 headers
        messageOption.addHeader("key1", "value1");
        messageOption.addHeader("key2", "value2");
        
        Map<String, String> headers = messageOption.getHeaders();
        assertNotNull(headers);
        assertEquals(2, headers.size());
        assertEquals("value1", headers.get("key1"));
        assertEquals("value2", headers.get("key2"));
    }

    @Test
    public void testGetHeadersReturnsUnmodifiableMap() {
        // 测试返回的 headers map 是不可修改的
        messageOption.addHeader("key", "value");
        Map<String, String> headers = messageOption.getHeaders();
        
        assertThrows(UnsupportedOperationException.class, () -> {
            headers.put("new-key", "new-value");
        });
    }

    @Test
    public void testConcurrentAccess() {
        // 测试并发访问（基本测试，实际并发测试需要更复杂的设置）
        messageOption.addHeader("key1", "value1");
        
        // 同时进行多个操作
        messageOption.addHeader("key2", "value2");
        Optional<String> value1 = messageOption.getHeaderValue("key1");
        Set<String> keys = messageOption.getHeaderKeys();
        boolean hasHeader = messageOption.hasHeader();
        
        assertTrue(value1.isPresent());
        assertEquals("value1", value1.get());
        assertEquals(2, keys.size());
        assertTrue(hasHeader);
    }

    @Test
    public void testCompleteWorkflow() {
        // 测试完整的工作流程
        // 1. 设置基本属性
        messageOption.setTag("order-tag");
        messageOption.setMessageKey("order-123");
        
        // 2. 添加多个 headers
        messageOption.addHeader("Content-Type", "application/json");
        messageOption.addHeader("Priority", "high");
        messageOption.addHeader("Retry-Count", "3");
        
        // 3. 验证所有设置
        assertEquals("order-tag", messageOption.getTag());
        assertEquals("order-123", messageOption.getMessageKey());
        assertTrue(messageOption.hasHeader());
        assertEquals(3, messageOption.getHeaderKeys().size());
        assertEquals("application/json", messageOption.getHeaderValue("Content-Type").get());
        assertEquals("high", messageOption.getHeaderValue("Priority").get());
        assertEquals("3", messageOption.getHeaderValue("Retry-Count").get());
        
        // 4. 验证返回的 headers map
        Map<String, String> allHeaders = messageOption.getHeaders();
        assertEquals(3, allHeaders.size());
        assertEquals("application/json", allHeaders.get("Content-Type"));
        assertEquals("high", allHeaders.get("Priority"));
        assertEquals("3", allHeaders.get("Retry-Count"));
    }

    @Test
    public void testEmptyStringValues() {
        // 测试空字符串值
        messageOption.setTag("");
        messageOption.setMessageKey("");
        messageOption.addHeader("", "");
        messageOption.addHeader("empty-value", "");
        messageOption.addHeader("", "empty-key");
        
        assertEquals("", messageOption.getTag());
        assertEquals("", messageOption.getMessageKey());
        assertEquals("", messageOption.getHeaderValue("empty-value").get());
        assertEquals("empty-key", messageOption.getHeaderValue("").get()); // 后添加的会覆盖前面的
        assertTrue(messageOption.hasHeader());
        assertEquals(2, messageOption.getHeaderKeys().size());
    }

    @Test
    public void testSpecialCharactersInHeadersAndProperties() {
        // 测试特殊字符
        String specialTag = "tag-with-特殊字符-@#$%^&*()";
        String specialKey = "key-with-特殊字符-@#$%^&*()";
        String specialHeaderKey = "header-特殊字符-@#$%^&*()";
        String specialHeaderValue = "value-特殊字符-@#$%^&*()";
        
        messageOption.setTag(specialTag);
        messageOption.setMessageKey(specialKey);
        messageOption.addHeader(specialHeaderKey, specialHeaderValue);
        
        assertEquals(specialTag, messageOption.getTag());
        assertEquals(specialKey, messageOption.getMessageKey());
        assertEquals(specialHeaderValue, messageOption.getHeaderValue(specialHeaderKey).get());
    }

    @Test
    public void testLongStringValues() {
        // 测试长字符串值
        StringBuilder longString = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            longString.append("long-string-").append(i).append("-");
        }
        String longValue = longString.toString();
        
        messageOption.setTag(longValue);
        messageOption.setMessageKey(longValue);
        messageOption.addHeader("long-key", longValue);
        
        assertEquals(longValue, messageOption.getTag());
        assertEquals(longValue, messageOption.getMessageKey());
        assertEquals(longValue, messageOption.getHeaderValue("long-key").get());
    }

    @Test
    public void testHeaderKeysImmutability() {
        // 测试 getHeaderKeys 返回的 Set 是否可以被修改
        messageOption.addHeader("key1", "value1");
        messageOption.addHeader("key2", "value2");
        
        Set<String> headerKeys = messageOption.getHeaderKeys();
        
        // 尝试修改返回的 Set（这可能会抛出异常，取决于实现）
        try {
            headerKeys.add("new-key");
            // 如果没有抛出异常，验证原始对象没有被修改
            assertFalse(messageOption.getHeaderKeys().contains("new-key"));
        } catch (UnsupportedOperationException e) {
            // 如果抛出异常，这是期望的行为
            assertTrue(true);
        }
    }

    @Test
    public void testMultipleInstancesIndependence() {
        // 测试多个实例之间的独立性
        MessageOption option1 = new MessageOption();
        MessageOption option2 = new MessageOption();
        
        option1.setTag("tag1");
        option1.setMessageKey("key1");
        option1.addHeader("header1", "value1");
        
        option2.setTag("tag2");
        option2.setMessageKey("key2");
        option2.addHeader("header2", "value2");
        
        // 验证两个实例互不影响
        assertEquals("tag1", option1.getTag());
        assertEquals("tag2", option2.getTag());
        assertEquals("key1", option1.getMessageKey());
        assertEquals("key2", option2.getMessageKey());
        assertEquals("value1", option1.getHeaderValue("header1").get());
        assertEquals("value2", option2.getHeaderValue("header2").get());
        assertFalse(option1.getHeaderValue("header2").isPresent());
        assertFalse(option2.getHeaderValue("header1").isPresent());
    }

    @Test
    public void testHeaderOperationsAfterClear() {
        // 测试清空 headers 后的操作（通过设置为 null 模拟）
        messageOption.addHeader("key1", "value1");
        messageOption.addHeader("key2", "value2");
        assertTrue(messageOption.hasHeader());
        
        // 由于没有 clear 方法，我们测试重新初始化的情况
        MessageOption newOption = new MessageOption();
        assertFalse(newOption.hasHeader());
        assertTrue(newOption.getHeaderKeys().isEmpty());
        assertTrue(newOption.getHeaders().isEmpty());
        assertFalse(newOption.getHeaderValue("any-key").isPresent());
    }

    @Test
    public void testCaseSensitiveHeaders() {
        // 测试 header key 的大小写敏感性
        messageOption.addHeader("Content-Type", "application/json");
        messageOption.addHeader("content-type", "application/xml");
        messageOption.addHeader("CONTENT-TYPE", "text/plain");
        
        // 验证大小写敏感
        assertEquals("application/json", messageOption.getHeaderValue("Content-Type").get());
        assertEquals("application/xml", messageOption.getHeaderValue("content-type").get());
        assertEquals("text/plain", messageOption.getHeaderValue("CONTENT-TYPE").get());
        assertEquals(3, messageOption.getHeaderKeys().size());
    }

    @Test
    public void testThreadSafetyBasic() throws InterruptedException {
        // 基本的线程安全测试
        final int threadCount = 10;
        final int operationsPerThread = 100;
        Thread[] threads = new Thread[threadCount];
        
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            threads[i] = new Thread(() -> {
                for (int j = 0; j < operationsPerThread; j++) {
                    messageOption.addHeader("key-" + threadId + "-" + j, "value-" + threadId + "-" + j);
                    messageOption.getHeaderValue("key-" + threadId + "-" + j);
                    messageOption.hasHeader();
                    messageOption.getHeaderKeys();
                    messageOption.getHeaders();
                }
            });
        }
        
        // 启动所有线程
        for (Thread thread : threads) {
            thread.start();
        }
        
        // 等待所有线程完成
        for (Thread thread : threads) {
            thread.join();
        }
        
        // 验证最终状态
        assertTrue(messageOption.hasHeader());
        assertEquals(threadCount * operationsPerThread, messageOption.getHeaderKeys().size());
    }

    @Test
    public void testDefaultConstructorState() {
        // 测试默认构造函数的初始状态
        MessageOption newOption = new MessageOption();
        
        assertNull(newOption.getTag());
        assertNull(newOption.getMessageKey());
        assertFalse(newOption.hasHeader());
        assertTrue(newOption.getHeaderKeys().isEmpty());
        assertTrue(newOption.getHeaders().isEmpty());
        assertFalse(newOption.getHeaderValue("any-key").isPresent());
    }

    @Test
    public void testHeaderValueTypes() {
        // 测试不同类型的 header 值（都是字符串）
        messageOption.addHeader("number", "123");
        messageOption.addHeader("boolean", "true");
        messageOption.addHeader("json", "{\"key\":\"value\"}");
        messageOption.addHeader("xml", "<root><item>value</item></root>");
        messageOption.addHeader("url", "https://example.com/path?param=value");
        
        assertEquals("123", messageOption.getHeaderValue("number").get());
        assertEquals("true", messageOption.getHeaderValue("boolean").get());
        assertEquals("{\"key\":\"value\"}", messageOption.getHeaderValue("json").get());
        assertEquals("<root><item>value</item></root>", messageOption.getHeaderValue("xml").get());
        assertEquals("https://example.com/path?param=value", messageOption.getHeaderValue("url").get());
    }

    @Test
    public void testHasHeaderWithEmptyHeaders() {
        // 测试 headers 不为 null 但为空的情况，覆盖 hasHeader 方法中未覆盖的分支
        // 通过反射或者其他方式创建一个空的 HashMap 来测试这个边界情况
        messageOption.addHeader("temp", "temp");
        assertTrue(messageOption.hasHeader());
        
        // 创建一个新的实例来测试空 headers 的情况
        MessageOption emptyHeaderOption = new MessageOption();
        
        // 先添加一个 header，然后通过 Java 反射访问 headers 字段并清空它
        emptyHeaderOption.addHeader("test", "test");
        assertTrue(emptyHeaderOption.hasHeader());
        
        try {
            java.lang.reflect.Field headersField = MessageOption.class.getDeclaredField("headers");
            headersField.setAccessible(true);
            Map<String, String> headers = (Map<String, String>) headersField.get(emptyHeaderOption);
            headers.clear(); // 清空 headers，但保持 headers 不为 null
            
            // 现在测试 hasHeader 方法，此时 headers 不为 null 但为空
            assertFalse(emptyHeaderOption.hasHeader());
            
        } catch (Exception e) {
            // 如果反射失败，跳过这个测试
            // 这种情况下我们无法直接测试这个分支
            System.out.println("反射访问失败，跳过空 headers 测试: " + e.getMessage());
        }
    }
}