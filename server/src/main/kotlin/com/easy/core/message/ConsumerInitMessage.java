package com.easy.core.message;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.Set;
@Getter
@Setter
@ToString
public class ConsumerInitMessage implements Serializable {

    private static final long serialVersionUID = 1L;


    String consumerGroupName;
    String consumerName;
    Set<String> listenedTopics;


    public ConsumerInitMessage(String consumerGroupName, String consumerName, Set<String> listenedTopics) {
        this.consumerGroupName = consumerGroupName;
        this.consumerName = consumerName;
        this.listenedTopics = listenedTopics;
    }

    public String getConsumerGroupName() {
        return consumerGroupName;
    }


}
