package com.github.wz2cool.localqueue;

public interface IWriter {

    /**
     * 写入
     *
     * @param message 消息
     * @return 是否成功，false 一般是队列满了
     */
    boolean offer(String message);
}
