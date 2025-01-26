package com.github.wz2cool.localqueue.model.message;

/**
 * queue message
 *
 * @author frank
 */
public class QueueMessage {

    private int positionVersion;
    private Long position;
    private String content;

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

    public void setPosition(Long position) {
        this.position = position;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public int getPositionVersion() {
        return positionVersion;
    }

    public void setPositionVersion(int positionVersion) {
        this.positionVersion = positionVersion;
    }
}
