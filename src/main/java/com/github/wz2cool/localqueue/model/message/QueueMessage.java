package com.github.wz2cool.localqueue.model.message;

/**
 * queue message
 *
 * @author frank
 */
public class QueueMessage {

    private final int positionVersion;
    private final Long position;
    private final String content;

    /**
     * constructor
     *
     * @param position position of queue
     * @param content  content
     */
    public QueueMessage(int positionVersion, Long position, String content) {
        this.positionVersion = positionVersion;
        this.position = position;
        this.content = content;
    }

    public Long getPosition() {
        return position;
    }

    public String getContent() {
        return content;
    }

    public int getPositionVersion() {
        return positionVersion;
    }
}
