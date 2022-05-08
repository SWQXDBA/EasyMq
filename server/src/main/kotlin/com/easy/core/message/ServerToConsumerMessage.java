package com.easy.core.message;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 服务器发送给消费者客户端 客户端直接收到的消息
 */
public class ServerToConsumerMessage implements Serializable {
    private static final long serialVersionUID = 1L;
    private List<ServerToConsumerMessageUnit> messages = new ArrayList<>();

    public List<ServerToConsumerMessageUnit> getMessages() {
        return messages;
    }

    public ServerToConsumerMessage() {
    }

    public void setMessages(List<ServerToConsumerMessageUnit> messages) {
        this.messages = messages;
    }

    public void putMessage(String topicName,TransmissionMessage messageUnit) {
        messages.add(new ServerToConsumerMessageUnit(topicName,messageUnit));
    }
}
