package com.github.wz2cool.localqueue.model.message;

public class BaseInternalMessage {

    protected long writeTime;
    protected String messageKey;
    protected String content;

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
}
