package com.github.wz2cool.localqueue.model.message;

/**
 * queue message
 *
 * @author frank
 */
public class QueueMessage {

    private Long position;
    private String content;

    /**
     * constructor
     *
     * @param position position of queue
     * @param content  content
     */
    public QueueMessage(Long position, String content) {
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
}
