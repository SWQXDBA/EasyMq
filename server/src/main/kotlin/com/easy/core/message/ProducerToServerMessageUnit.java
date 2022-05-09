package com.easy.core.message;

import java.io.Serializable;
import java.util.Arrays;

public class ProducerToServerMessageUnit implements Serializable {


    private static final long serialVersionUID = 1L;
    public ProducerToServerMessageUnit(Long messageProductionNumber, byte[] data, String topicName,Class<?> dataClass) {
        this.messageProductionNumber = messageProductionNumber;
        this.data = data;
        this.topicName = topicName;
        this.dataClass = dataClass;
    }

    /**
     * 由每个producer自己维护的一个id 供服务端拿来避免重复接收 使用
     */
    public  Long messageProductionNumber;

    public byte[] data;

    public String topicName;

    /**
     * 发送时 data的类型
     */
    Class<?> dataClass;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ProducerToServerMessageUnit)) return false;

        ProducerToServerMessageUnit unit = (ProducerToServerMessageUnit) o;

        if (messageProductionNumber != null ? !messageProductionNumber.equals(unit.messageProductionNumber) : unit.messageProductionNumber != null)
            return false;
        if (!Arrays.equals(data, unit.data)) return false;
        return topicName != null ? topicName.equals(unit.topicName) : unit.topicName == null;
    }

    @Override
    public int hashCode() {
        int result = messageProductionNumber != null ? messageProductionNumber.hashCode() : 0;
        result = 31 * result + Arrays.hashCode(data);
        result = 31 * result + (topicName != null ? topicName.hashCode() : 0);
        return result;
    }


    public Long getMessageProductionNumber() {
        return messageProductionNumber;
    }

    public void setMessageProductionNumber(Long messageProductionNumber) {
        this.messageProductionNumber = messageProductionNumber;
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
