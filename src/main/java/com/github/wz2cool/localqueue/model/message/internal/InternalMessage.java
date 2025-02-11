package com.github.wz2cool.localqueue.model.message.internal;

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.InvalidMarshallableException;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.util.Set;

public class InternalMessage implements BytesMarshallable {

    private final Set<String> matchTags;
    private final boolean ignoreReadContent;

    private String tag;
    private long writeTime;
    private String messageKey;
    private String content;

    private HeaderMessage headerMessage;

    public InternalMessage() {
        this.matchTags = null;
        this.ignoreReadContent = false;
    }

    public InternalMessage(boolean ignoreReadContent) {
        this.matchTags = null;
        this.ignoreReadContent = ignoreReadContent;
    }

    public InternalMessage(Set<String> matchTags) {
        this.matchTags = matchTags;
        this.ignoreReadContent = false;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public long getWriteTime() {
        return writeTime;
    }

    public void setWriteTime(long writeTime) {
        this.writeTime = writeTime;
    }

    public String getMessageKey() {
        return messageKey;
    }

    public void setMessageKey(String messageKey) {
        this.messageKey = messageKey;
    }

    public HeaderMessage getHeaderMessage() {
        return headerMessage;
    }

    public void setHeaderMessage(HeaderMessage headerMessage) {
        this.headerMessage = headerMessage;
    }

    @Override
    public void readMarshallable(BytesIn<?> bytes) throws IORuntimeException, BufferUnderflowException, IllegalStateException, InvalidMarshallableException {
        this.tag = bytes.readUtf8();
        String messageTag = tag == null ? "*" : tag;
        if (matchTags == null || matchTags.contains("*") || matchTags.contains(messageTag)) {
            this.writeTime = bytes.readLong();
            this.messageKey = bytes.readUtf8();
            if (!ignoreReadContent) {
                this.content = bytes.readUtf8();
                this.headerMessage = bytes.readObject(HeaderMessage.class);
            }
        }
    }

    @Override
    public void writeMarshallable(BytesOut<?> bytes) throws IllegalStateException, BufferOverflowException, BufferUnderflowException, ArithmeticException, InvalidMarshallableException {
        bytes.writeUtf8(this.tag);
        bytes.writeLong(this.writeTime);
        bytes.writeUtf8(this.messageKey);
        bytes.writeUtf8(this.content);
        // must write empty header message to make sure the header message is not null.
        HeaderMessage useHeaderMessage = this.headerMessage == null ? new HeaderMessage(null) : this.headerMessage;
        bytes.writeObject(HeaderMessage.class, useHeaderMessage);
    }
}
