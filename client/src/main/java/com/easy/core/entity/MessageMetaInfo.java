package com.easy.core.entity;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 消息的元数据。该信息每一段时间进行一次持久化以尽量保证不会重复发送消息
 * 但是消息接收方应该自行保证不会重复消费。
 *
 * @author SWQXDBA
 */
public class MessageMetaInfo  implements Serializable {
    private static final long serialVersionUID = 1L;
    String topicName;
    /**
     * 未消费完成的消费组 用于消息重发
     */
    ConcurrentHashMap<MessageId, Set<ConsumerGroup>> unconsumedGroups = new ConcurrentHashMap<>();

    /**
     * 消息发送时间
     */
    ConcurrentHashMap<MessageId, HashMap<ConsumerGroup, LocalDateTime>> consumesSendTime = new ConcurrentHashMap<>();

    /**
     *   用来告诉producer 此消息已经被服务器接收过
     */
    Set<MessageId>  receivedMessages = ConcurrentHashMap.newKeySet();
}
