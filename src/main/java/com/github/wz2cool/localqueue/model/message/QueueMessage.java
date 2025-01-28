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

    /**
     * constructor
     *
     * @param position position of queue
     * @param content  content
     */
    public QueueMessage(int positionVersion, long position, String content, long writeTime) {
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
}
