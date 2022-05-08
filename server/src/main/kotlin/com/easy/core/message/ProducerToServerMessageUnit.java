package com.easy.core.message;

import java.util.Arrays;
import java.util.Objects;

public class ProducerToServerMessageUnit {
    /**
     * 由每个producer自己维护的一个id 供服务端拿来避免重复接收 使用
     */
    Long messageProductionNumber;
    public byte[] data;
    String topicName;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProducerToServerMessageUnit that = (ProducerToServerMessageUnit) o;
        return Objects.equals(messageProductionNumber, that.messageProductionNumber) && Arrays.equals(data, that.data) && Objects.equals(topicName, that.topicName);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(messageProductionNumber, topicName);
        result = 31 * result + Arrays.hashCode(data);
        return result;
    }
}
