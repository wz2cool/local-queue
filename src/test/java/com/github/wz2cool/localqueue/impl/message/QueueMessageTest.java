package com.github.wz2cool.localqueue.impl.message;

import com.github.wz2cool.localqueue.model.message.QueueMessage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@SuppressWarnings("all")
public class QueueMessageTest {

    @Test
    public void testConstructorAndGetters() {
        // 创建一个 QueueMessage 实例
        int positionVersion = 1;
        Long position = 100L;
        String content = "Hello, World!";

        QueueMessage message = new QueueMessage(positionVersion, position, content);

        // 验证字段是否正确
        assertEquals(positionVersion, message.getPositionVersion());
        assertEquals(position, message.getPosition());
        assertEquals(content, message.getContent());
    }

    @Test
    public void testConstructorWithNullContent() {
        // 创建一个 QueueMessage 实例，内容为 null
        int positionVersion = 1;
        Long position = 100L;
        String content = null;

        QueueMessage message = new QueueMessage(positionVersion, position, content);

        // 验证字段是否正确
        assertEquals(positionVersion, message.getPositionVersion());
        assertEquals(position, message.getPosition());
        assertNull(message.getContent());
    }

    @Test
    public void testConstructorWithZeroPosition() {
        // 创建一个 QueueMessage 实例，位置为 0
        int positionVersion = 1;
        Long position = 0L;
        String content = "Hello, World!";

        QueueMessage message = new QueueMessage(positionVersion, position, content);

        // 验证字段是否正确
        assertEquals(positionVersion, message.getPositionVersion());
        assertEquals(position, message.getPosition());
        assertEquals(content, message.getContent());
    }

    @Test
    public void testConstructorWithNegativePosition() {
        // 创建一个 QueueMessage 实例，位置为负数
        int positionVersion = 1;
        Long position = -1L;
        String content = "Hello, World!";

        QueueMessage message = new QueueMessage(positionVersion, position, content);

        // 验证字段是否正确
        assertEquals(positionVersion, message.getPositionVersion());
        assertEquals(position, message.getPosition());
        assertEquals(content, message.getContent());
    }

    @Test
    public void testConstructorWithZeroPositionVersion() {
        // 创建一个 QueueMessage 实例，位置版本为 0
        int positionVersion = 0;
        Long position = 100L;
        String content = "Hello, World!";

        QueueMessage message = new QueueMessage(positionVersion, position, content);

        // 验证字段是否正确
        assertEquals(positionVersion, message.getPositionVersion());
        assertEquals(position, message.getPosition());
        assertEquals(content, message.getContent());
    }

    @Test
    public void testConstructorWithNegativePositionVersion() {
        // 创建一个 QueueMessage 实例，位置版本为负数
        int positionVersion = -1;
        Long position = 100L;
        String content = "Hello, World!";

        QueueMessage message = new QueueMessage(positionVersion, position, content);

        // 验证字段是否正确
        assertEquals(positionVersion, message.getPositionVersion());
        assertEquals(position, message.getPosition());
        assertEquals(content, message.getContent());
    }
}
