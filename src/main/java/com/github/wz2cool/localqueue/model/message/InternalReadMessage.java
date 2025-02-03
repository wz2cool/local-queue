package com.github.wz2cool.localqueue.model.message;

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.InvalidMarshallableException;

import java.nio.BufferUnderflowException;
import java.util.Set;

public class InternalReadMessage extends BaseInternalMessage implements ReadBytesMarshallable {

    private final boolean ignoreReadContent;
    private final Set<String> tags;

    public InternalReadMessage() {
        this.ignoreReadContent = false;
        this.tags = null;
    }

    public InternalReadMessage(boolean ignoreReadContent) {
        this.ignoreReadContent = ignoreReadContent;
        this.tags = null;
    }

    public InternalReadMessage(Set<String> tags) {
        this.tags = tags;
        this.ignoreReadContent = false;
    }

    @Override
    public void readMarshallable(BytesIn<?> bytes) throws IORuntimeException, BufferUnderflowException, IllegalStateException, InvalidMarshallableException {
        this.writeTime = bytes.readLong();
        this.tag = bytes.readUtf8();
        String messageTag = tag == null ? "*" : tag;
        if (tags == null || tags.contains("*") || tags.contains(messageTag)) {
            this.messageKey = bytes.readUtf8();
            if (!ignoreReadContent) {
                this.content = bytes.readUtf8();
            }
        }
    }
}
