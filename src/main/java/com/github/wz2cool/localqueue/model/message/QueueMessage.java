package com.github.wz2cool.localqueue.model.message;

import com.github.wz2cool.localqueue.model.message.internal.HeaderMessage;

import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

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
    private final String tag;
    private final HeaderMessage headerMessage;

    /**
     * constructor
     *
     * @param tag             tag
     * @param messageKey      message key
     * @param positionVersion position version
     * @param position        position of queue
     * @param content         content
     * @param writeTime       write time
     */
    public QueueMessage(String tag, String messageKey, int positionVersion, long position, String content, long writeTime, HeaderMessage headerMessage) {
        this.tag = tag;
        this.messageKey = messageKey;
        this.positionVersion = positionVersion;
        this.position = position;
        this.content = content;
        this.writeTime = writeTime;
        this.headerMessage = headerMessage;
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

    public String getTag() {
        return tag;
    }

    public Optional<String> getHeaderValue(String headerKey) {
        if (Objects.isNull(headerMessage)) {
            return Optional.empty();
        }
        return headerMessage.getHeaderValue(headerKey);
    }

    public Set<String> getHeaderKeys() {
        if (Objects.isNull(headerMessage)) {
            return new HashSet<>();
        }
        return headerMessage.getHeaderKeys();
    }
}
