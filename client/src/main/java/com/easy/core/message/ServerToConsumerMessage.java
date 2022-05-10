package com.easy.core.message;

import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 服务器发送给消费者客户端 客户端直接收到的消息
 */
public class ServerToConsumerMessage implements Serializable {
    private static final long serialVersionUID = 1L;
    private Collection<TransmissionMessage> messages = ConcurrentHashMap.newKeySet(8);

    public Collection<TransmissionMessage> getMessages() {
        return messages;
    }

    public ServerToConsumerMessage() {
    }

    public void setMessages(Collection<TransmissionMessage> messages) {
        this.messages = messages;
    }

    public void putMessage(TransmissionMessage messageUnit) {
        messages.add(messageUnit);
    }
}
