package com.easy.core.entity;

import java.io.Serializable;
import java.util.Objects;

public class MessageId implements Serializable {
    private static final long serialVersionUID = 1L;

    String topicName;
    String producerName;
    long uid;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MessageId messageId = (MessageId) o;
        return uid == messageId.uid && Objects.equals(topicName, messageId.topicName) && Objects.equals(producerName, messageId.producerName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicName, producerName, uid);
    }

    public MessageId(String topicName, String producerName, long uid) {
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

    public long getUid() {
        return uid;
    }

    public void setUid(long uid) {
        this.uid = uid;
    }
}
