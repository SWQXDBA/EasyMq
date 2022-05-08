package com.easy.core.message;

import com.easy.core.enums.ClientMessageType;
import com.easy.core.enums.CodeType;

import java.util.Set;

public class ConsumerToServerMessage {
    Long id;
    /**
     * 这个消息的作用
     */
    ClientMessageType messageType;
    /**
     * 序列化方式
     */
    CodeType codeType;

    /**
     * 请求的消息数量
     */
    Integer requestMessageCount;

    /**
     * 告诉服务器 这些消息已被收到
     */
    Set<Long> receivedIds;


}
