package com.github.wz2cool.localqueue.model.message.internal;

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.InvalidMarshallableException;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class HeaderMessage implements BytesMarshallable {

    private final Map<String, String> headers = new HashMap<>();

    public synchronized Optional<String> getHeaderValue(String headerKey) {
        return Optional.ofNullable(headers.get(headerKey));
    }

    public synchronized Set<String> getHeaderKeys() {
        return headers.keySet();
    }

    public synchronized Map<String, String> getHeaders() {
        return headers;
    }

    @Override
    public void readMarshallable(BytesIn<?> bytes) throws IORuntimeException, BufferUnderflowException, IllegalStateException, InvalidMarshallableException {
        int mapSize = bytes.readInt();
        if (mapSize > 0) {
            for (int i = 0; i < mapSize; i++) {
                String key = bytes.readUtf8();
                String value = bytes.readUtf8();
                headers.put(key, value);
            }
        }
    }

    @Override
    public void writeMarshallable(BytesOut<?> bytes) throws IllegalStateException, BufferOverflowException, BufferUnderflowException, ArithmeticException, InvalidMarshallableException {
        bytes.writeInt(headers.size());
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            bytes.writeUtf8(entry.getKey());
            bytes.writeUtf8(entry.getValue());
        }
    }
}
