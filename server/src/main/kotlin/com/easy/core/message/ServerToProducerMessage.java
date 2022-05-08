package com.easy.core.message;

import java.util.Set;

public class ServerToProducerMessage {
    /**
     * 已经收到的消息id
     */
    Set<Long> receivedIds;
}
