package com.github.wz2cool.localqueue.model.message;

/**
 * queue message
 *
 * @author frank
 */
public class QueueMessage {

    private final int positionVersion;
    private final long position;
    private final String content;
    private final long writeTime;
    private final String messageKey;

    /**
     * constructor
     *
     * @param messageKey      message key
     * @param positionVersion position version
     * @param position        position of queue
     * @param content         content
     * @param writeTime       write time
     */
    public QueueMessage(String messageKey, int positionVersion, long position, String content, long writeTime) {
        this.messageKey = messageKey;
        this.positionVersion = positionVersion;
        this.position = position;
        this.content = content;
        this.writeTime = writeTime;
    }

    public long getPosition() {
        return position;
    }

    public String getContent() {
        return content;
    }

    public int getPositionVersion() {
        return positionVersion;
    }

    public long getWriteTime() {
        return writeTime;
    }

    public String getMessageKey() {
        return messageKey;
    }
}
