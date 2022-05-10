package com.easy.core.message;

import com.easy.core.entity.MessageId;

import java.io.Serializable;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ConsumerToServerMessage implements Serializable {
    private static final long serialVersionUID = 1L;
    public Set<MessageId> confirmationResponse = ConcurrentHashMap.newKeySet();
    public String consumerGroupName;

    public ConsumerToServerMessage(String consumerGroupName) {
        this.consumerGroupName = consumerGroupName;
    }
}
