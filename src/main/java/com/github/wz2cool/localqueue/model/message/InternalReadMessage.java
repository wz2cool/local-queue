package com.github.wz2cool.localqueue.model.message;

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.InvalidMarshallableException;

import java.nio.BufferUnderflowException;

public class InternalReadMessage extends BaseInternalMessage implements ReadBytesMarshallable {

    private final boolean ignoreReadContent;

    public InternalReadMessage() {
        this.ignoreReadContent = false;
    }

    public InternalReadMessage(boolean ignoreReadContent) {
        this.ignoreReadContent = ignoreReadContent;
    }

    @Override
    public void readMarshallable(BytesIn<?> bytes) throws IORuntimeException, BufferUnderflowException, IllegalStateException, InvalidMarshallableException {
        this.writeTime = bytes.readLong();
        this.tag = bytes.readUtf8();
        this.messageKey = bytes.readUtf8();
        if (!ignoreReadContent) {
            this.content = bytes.readUtf8();
        }
    }
}
