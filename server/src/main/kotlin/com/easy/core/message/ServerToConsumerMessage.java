package com.easy.core.message;

import java.util.ArrayList;
import java.util.List;

/**
 * 服务器发送给消费者客户端 客户端直接收到的消息
 */
public class ServerToConsumerMessage {

    private List<TransmissionMessage> messages = new ArrayList<>();


    public void putMessage(TransmissionMessage messageUnit) {
        messages.add(messageUnit);
    }
}
