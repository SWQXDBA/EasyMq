package com.easy.core.message;

import java.io.Serializable;

public class ProducerToServerMessageUnit implements Serializable {

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ProducerToServerMessageUnit)) return false;

        ProducerToServerMessageUnit unit = (ProducerToServerMessageUnit) o;

        return messageProductionId != null ? messageProductionId.equals(unit.messageProductionId) : unit.messageProductionId == null;
    }

    @Override
    public int hashCode() {
        return messageProductionId != null ? messageProductionId.hashCode() : 0;
    }

    private static final long serialVersionUID = 1L;
    public ProducerToServerMessageUnit(String messageProductionNumber, byte[] data, String topicName,Class<?> dataClass) {
        this.messageProductionId = messageProductionNumber;
        this.data = data;
        this.topicName = topicName;
        this.dataClass = dataClass;
    }

    /**
     * 由每个producer自己维护的一个id 供服务端拿来避免重复接收 使用
     */
    public  String messageProductionId;

    public byte[] data;

    public String topicName;

    /**
     * 发送时 data的类型
     */
    Class<?> dataClass;


    public String getMessageProductionId() {
        return messageProductionId;
    }

    public void setMessageProductionId(String messageProductionId) {
        this.messageProductionId = messageProductionId;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public Class<?> getDataClass() {
        return dataClass;
    }

    public void setDataClass(Class<?> dataClass) {
        this.dataClass = dataClass;
    }
}
