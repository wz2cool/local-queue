package com.github.wz2cool.localqueue.impl.demo;

import com.github.wz2cool.localqueue.IConsumer;
import com.github.wz2cool.localqueue.impl.SimpleQueue;
import com.github.wz2cool.localqueue.model.config.SimpleQueueConfig;
import com.github.wz2cool.localqueue.model.enums.ConsumeFromWhere;
import com.github.wz2cool.localqueue.model.message.QueueMessage;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SimpleQueueDemo {

    private File dir;
    private SimpleQueueConfig config;

    @BeforeEach
    public void setUp() throws IOException {
        dir = new File("./test");
        FileUtils.deleteDirectory(dir);
        config = new SimpleQueueConfig.Builder()
                .setDataDir(dir)
                .setKeepDays(1)
                .build();
    }

    @AfterEach
    public void cleanUp() throws IOException {
        FileUtils.deleteDirectory(dir);
    }

    @Test
    public void testMultiConsumers() throws InterruptedException {
        try (SimpleQueue queue = new SimpleQueue(config)) {
            Thread consumer1Thread = new Thread(() -> {
                IConsumer consumer1 = queue.getConsumer("consumer1");
                try {
                    // 阻塞式获取，
                    QueueMessage take = consumer1.take();
                    assertEquals("testContent", take.getContent());
                    assertEquals("testMessageKey", take.getMessageKey());
                    System.out.println("consumer1 take message");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            Thread consumer2Thread = new Thread(() -> {
                IConsumer consumer2 = queue.getConsumer("consumer2");
                try {
                    // 阻塞式获取，
                    QueueMessage take = consumer2.take();
                    assertEquals("testContent", take.getContent());
                    assertEquals("testMessageKey", take.getMessageKey());
                    System.out.println("consumer2 take message");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            consumer1Thread.start();
            consumer2Thread.start();
            // 确保两个消费者开始读准备好了
            Thread.sleep(2000);
            // 写入消息
            queue.offer("testMessageKey", "testContent");
            consumer1Thread.join();
            consumer2Thread.join();
        }
    }

    @Test
    public void testResumeConsumer() throws InterruptedException {
        try (SimpleQueue queue = new SimpleQueue(config)) {
            for (int i = 0; i < 10; i++) {
                String key = "key" + i;
                String content = "content" + i;
                queue.offer(key, content);
            }
            // 确保消息全部写入
            Thread.sleep(100);
            String consumerId = "consumer1";
            // 从最早的位置开始读，（默认是最新的，如果不用First 会读不到消息），
            // 这里我们是需要关闭所以用了try-with-resources, 一般不需要在queue 这释放
            try (IConsumer consumer1 = queue.getConsumer(consumerId, ConsumeFromWhere.FIRST)) {
                // 确保消息写入缓存
                Thread.sleep(100);
                List<QueueMessage> queueMessages = consumer1.batchTake(5);
                assertEquals(5, queueMessages.size());
                QueueMessage lastOne = queueMessages.get(queueMessages.size() - 1);
                assertEquals("key4", lastOne.getMessageKey());
                consumer1.ack(queueMessages);
                // 确保位置写入
                Thread.sleep(100);
            }
            // 上面的消费者关闭以后，新开一个同一个consumerId消费者断点续读
            try (IConsumer consumer1 = queue.getConsumer(consumerId, ConsumeFromWhere.FIRST)) {
                // 确保消息写入缓存
                Thread.sleep(100);
                List<QueueMessage> queueMessages = consumer1.batchTake(3);
                assertEquals(3, queueMessages.size());
                QueueMessage lastOne = queueMessages.get(queueMessages.size() - 1);
                assertEquals("key7", lastOne.getMessageKey());
                consumer1.ack(queueMessages);
                // 确保位置写入
                Thread.sleep(100);
            }

        }
    }

    @Test
    public void testMoveToPosition() throws InterruptedException {
        try (SimpleQueue queue = new SimpleQueue(config)) {
            for (int i = 0; i < 10; i++) {
                String key = "key" + i;
                String content = "content" + i;
                queue.offer(key, content);
                Thread.sleep(100);
            }
            IConsumer consumer1 = queue.getConsumer("consumer1", ConsumeFromWhere.FIRST);
            // 确保消费者缓存填充完毕
            Thread.sleep(100);
            List<QueueMessage> queueMessages = consumer1.batchTake(10);
            // 尝试取第三位
            QueueMessage example = queueMessages.get(3);
            consumer1.ack(queueMessages);
            // 确保ack位点提交
            Thread.sleep(100);
            // 移动位置到第三个消息位置
            boolean moveToResult = consumer1.moveToPosition(example.getPosition());
            assertTrue(moveToResult);
            // 移动完成以后接着读
            QueueMessage takeMessage = consumer1.take();
            assertEquals(example.getPosition(), takeMessage.getPosition());
            assertEquals(example.getMessageKey(), takeMessage.getMessageKey());
        }
    }
}
