package com.github.wz2cool.localqueue.model.message;

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.InvalidMarshallableException;

import java.nio.BufferUnderflowException;

public class InternalReadMessage extends BaseInternalMessage implements ReadBytesMarshallable {

    @Override
    public void readMarshallable(BytesIn<?> bytes) throws IORuntimeException, BufferUnderflowException, IllegalStateException, InvalidMarshallableException {
        this.content = bytes.readUtf8();
        this.writeTime = bytes.readLong();
        this.extra = bytes.readUtf8();
    }
}
