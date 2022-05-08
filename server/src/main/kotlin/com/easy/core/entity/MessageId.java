package com.easy.core.entity;

import java.util.Objects;

public class MessageId {
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
}
