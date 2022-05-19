package com.easy.core.listener;

import com.easy.core.entity.MessageId;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * 注册到Client中 用于处理消息
 *
 * @param
 */
public abstract class DefaultListener<T> extends EasyListener {


    public DefaultListener(String topicName) {
        this.topicName = topicName;
        this.messageType = getMessageType();
    }

    private Class<T> getMessageType() {
        @SuppressWarnings("rawtypes")
        Class clazz = getClass();

        Class res = null;
        while (clazz != Object.class) {
            Type t = clazz.getGenericSuperclass();
            if (t instanceof ParameterizedType) {
                Type[] args = ((ParameterizedType) t).getActualTypeArguments();
                if (args[0] instanceof Class) {
                    return  (Class<T>) args[0];
                }
            }
            clazz = clazz.getSuperclass();
        }


        throw new NullPointerException("类型参数为null");

    }

    /**
     * 如何处理消息
     *
     * @param messageId
     * @param message
     */
    public abstract void handle(MessageId messageId, T message);


}
