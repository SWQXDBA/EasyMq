package com.easy.core.message;

import java.io.Serializable;
import java.util.Set;

public class ConsumerInitMessage implements Serializable {
    public ConsumerInitMessage(String consumerGroupName, Set<String> listenedTopics) {
        this.consumerGroupName = consumerGroupName;
        this.listenedTopics = listenedTopics;
    }

    public ConsumerInitMessage() {
    }

    public String getConsumerGroupName() {
        return consumerGroupName;
    }

    public void setConsumerGroupName(String consumerGroupName) {
        this.consumerGroupName = consumerGroupName;
    }

    public Set<String> getListenedTopics() {
        return listenedTopics;
    }

    public void setListenedTopics(Set<String> listenedTopics) {
        this.listenedTopics = listenedTopics;
    }

    private static final long serialVersionUID = 1L;
    String consumerGroupName;
    Set<String> listenedTopics;
}
