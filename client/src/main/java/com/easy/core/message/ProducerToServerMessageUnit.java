package com.easy.core.message;

import com.easy.core.entity.MessageId;

import java.io.Serializable;

public class ProducerToServerMessageUnit implements Serializable {

    private static final long serialVersionUID = 1L;

    public MessageId messageId;

    public byte[] data;

    /**
     * 是否需要接收回调消息
     */
    public boolean callBack;

    /**
     * 发送时 data的类型
     */
    Class<?> dataClass;

    public ProducerToServerMessageUnit(MessageId messageProductionNumber, byte[] data,Class<?> dataClass) {
        this.messageId = messageProductionNumber;
        this.data = data;
        this.dataClass = dataClass;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ProducerToServerMessageUnit)) return false;

        ProducerToServerMessageUnit unit = (ProducerToServerMessageUnit) o;

        return messageId != null ? messageId.equals(unit.messageId) : unit.messageId == null;
    }

    @Override
    public int hashCode() {
        return messageId != null ? messageId.hashCode() : 0;
    }

    public MessageId getMessageId() {
        return messageId;
    }

    public void setMessageId(MessageId messageId) {
        this.messageId = messageId;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public boolean isCallBack() {
        return callBack;
    }

    public void setCallBack(boolean callBack) {
        this.callBack = callBack;
    }

    public Class<?> getDataClass() {
        return dataClass;
    }

    public void setDataClass(Class<?> dataClass) {
        this.dataClass = dataClass;
    }
}
