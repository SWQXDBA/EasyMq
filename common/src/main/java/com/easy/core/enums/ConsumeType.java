package com.easy.core.enums;

public enum ConsumeType {

    /**
     * 还未被消费 是消息的默认状态
     */
    UNCONSUMED,
    /**
     * 正在消费 表示已经把消息发送给消费者了，但是还未收到消费者的答复
     */
    CONSUMING,
    /**
     * 已消费成功 表示已经把消息发送给消费者了，并且收到了答复
     */
    CONSUMED


}
