package com.github.wz2cool.localqueue.model.message;

import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.core.io.InvalidMarshallableException;

import java.nio.BufferOverflowException;

public class InternalWriteMessage extends BaseInternalMessage implements WriteBytesMarshallable {
    @Override
    public void writeMarshallable(BytesOut<?> bytes) throws IllegalStateException, BufferOverflowException, InvalidMarshallableException {
        bytes.writeLong(this.writeTime);
        bytes.writeUtf8(this.messageKey);
        bytes.writeUtf8(this.content);
    }
}
