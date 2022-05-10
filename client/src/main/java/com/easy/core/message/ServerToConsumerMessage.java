package com.easy.core.message;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 服务器发送给消费者客户端 客户端直接收到的消息
 */
public class ServerToConsumerMessage implements Serializable {
    private static final long serialVersionUID = 1L;
    private List<TransmissionMessage> messages = new LinkedList<>();

    public List<TransmissionMessage> getMessages() {
        return messages;
    }

    public ServerToConsumerMessage() {
    }

    public void setMessages(List<TransmissionMessage> messages) {
        this.messages = messages;
    }

    public void putMessage(TransmissionMessage messageUnit) {
        messages.add(messageUnit);
    }
}
