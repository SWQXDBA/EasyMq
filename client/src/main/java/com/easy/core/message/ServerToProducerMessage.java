package com.easy.core.message;

import com.easy.core.entity.MessageId;

import java.util.HashSet;
import java.util.Set;

public class ServerToProducerMessage {
    /**
     * 已经收到的消息id
     */
   public Set<MessageId> receivedIds = new HashSet<>();
}
