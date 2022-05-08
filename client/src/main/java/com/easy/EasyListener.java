package com.easy;

import com.easy.core.entity.MessageId;

/**
 * 注册到Client中 用于处理消息
 * @param <T>
 */
public abstract class EasyListener {
    public EasyListener(Class messageClass) {
        this.messageClass = messageClass;
    }

  public   Class messageClass;
    /**
     * 如何处理消息
     * @param messageId
     * @param message
     */
    public abstract void handle(MessageId messageId, Object message);
}
