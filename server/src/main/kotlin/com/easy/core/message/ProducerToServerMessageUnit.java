package com.easy.core.message;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
@Getter
@Setter
public class ProducerToServerMessageUnit implements Serializable {
    private static final long serialVersionUID = 1L;
    public ProducerToServerMessageUnit(Long messageProductionNumber, byte[] data, String topicName) {
        this.messageProductionNumber = messageProductionNumber;
        this.data = data;
        this.topicName = topicName;
    }

    /**
     * 由每个producer自己维护的一个id 供服务端拿来避免重复接收 使用
     */
    public  Long messageProductionNumber;
    public byte[] data;
    public String topicName;

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
}
