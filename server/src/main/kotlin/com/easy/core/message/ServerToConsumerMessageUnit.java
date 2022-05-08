package com.easy.core.message;

import java.io.Serializable;

/**
 * 仅仅提供了额外的topicName信息
 */
public class ServerToConsumerMessageUnit implements Serializable {
    private static final long serialVersionUID = 1L;
    String topicName;
    TransmissionMessage message;

    public ServerToConsumerMessageUnit(String topicName, TransmissionMessage message) {
        this.topicName = topicName;
        this.message = message;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public TransmissionMessage getMessage() {
        return message;
    }

    public void setMessage(TransmissionMessage message) {
        this.message = message;
    }
}
