package com.easy.core.listener;

import com.easy.core.entity.MessageId;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public abstract class CallBackListener<T,C> extends EasyListener {

    public CallBackListener(String topicName) {
        this.topicName = topicName;
        getTypes();
    }



    public Class<C> callBackType;
    private void getTypes() {
        @SuppressWarnings("rawtypes")
        Class clazz = getClass();

        Class res = null;
        while (clazz != Object.class) {
            Type t = clazz.getGenericSuperclass();
            if (t instanceof ParameterizedType) {
                Type[] args = ((ParameterizedType) t).getActualTypeArguments();
                if (args[0] instanceof Class) {
                    messageType =  (Class<T>) args[0];
                }
                if (args[1] instanceof Class) {
                    callBackType =  (Class<C>) args[1];
                }
                return;
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
    public abstract C answer(MessageId messageId, T message);

}
