package com.easy.core.entity;

import java.io.Serializable;
import java.util.Objects;

public class MessageId implements Serializable {
    private static final long serialVersionUID = 1L;

    String topicName;

    String producerName;
    /**
     * 全局唯的id 由消息生产者提供
     */
    String uid;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MessageId messageId = (MessageId) o;
        return uid.equals(messageId.uid) && Objects.equals(topicName, messageId.topicName) && Objects.equals(producerName, messageId.producerName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicName, producerName, uid);
    }

    public MessageId(String topicName, String producerName, String uid) {
        this.topicName = topicName;
        this.producerName = producerName;
        this.uid = uid;
    }

    public MessageId() {
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public String getProducerName() {
        return producerName;
    }

    public void setProducerName(String producerName) {
        this.producerName = producerName;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }
}
