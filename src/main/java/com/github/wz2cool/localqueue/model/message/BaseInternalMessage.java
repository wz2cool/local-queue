package com.github.wz2cool.localqueue.model.message;

public class BaseInternalMessage {

    protected String content;
    protected long writeTime;

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
}
