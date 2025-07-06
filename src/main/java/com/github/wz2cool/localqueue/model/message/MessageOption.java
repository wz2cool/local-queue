package com.github.wz2cool.localqueue.model.message;

import java.util.*;

/**
 * 消息配置
 *
 * @author frank
 */
public class MessageOption {

    private Map<String, String> headers;

    /**
     * message tag
     */
    private String tag;
    /**
     * message key
     */
    private String messageKey;

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getMessageKey() {
        return messageKey;
    }

    public void setMessageKey(String messageKey) {
        this.messageKey = messageKey;
    }

    /**
     * add header
     *
     * @param key   header key
     * @param value header value
     */
    public synchronized void addHeader(String key, String value) {
        if (headers == null) {
            headers = new HashMap<>();
        }
        headers.put(key, value);
    }

    /**
     * 获取消息头
     *
     * @param headerKey header key
     * @return header value
     */
    public synchronized Optional<String> getHeaderValue(String headerKey) {
        if (Objects.isNull(headers)) {
            return Optional.empty();
        }
        return Optional.ofNullable(headers.get(headerKey));
    }

    /**
     * get header keys
     *
     * @return header keys
     */
    public synchronized Set<String> getHeaderKeys() {
        if (Objects.isNull(headers)) {
            return new HashSet<>();
        }
        return headers.keySet();
    }

    /**
     * check if has header
     *
     * @return true if has header
     */
    public synchronized boolean hasHeader() {
        if (Objects.isNull(headers)) {
            return false;
        }
        return !headers.isEmpty();
    }

    /**
     * get header keys
     *
     * @return header keys
     */
    public synchronized Map<String, String> getHeaders() {
        if (Objects.isNull(headers)) {
            return new HashMap<>();
        }
        return Collections.unmodifiableMap(headers);
    }
}
