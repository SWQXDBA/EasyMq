package com.easy.core.entity;

import com.easy.core.ConsumerGroup;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 消息的元数据。该信息每一段时间进行一次持久化以尽量保证不会重复发送消息
 * 但是消息接收方应该自行保证不会重复消费。
 * @author SWQXDBA
 */
public class MessageMetaInfo {
    String topicName;
    /**
     * 用于记录所有消费者类别对该消息的消费状态
     * <消费类名,状态枚举>
     */
    ConcurrentHashMap<MessageId, Set<ConsumerGroup>> consumedGroups = new ConcurrentHashMap<>();

    /**
     * 消息发送时间
     */
    ConcurrentHashMap<MessageId, HashMap<ConsumerGroup, LocalDateTime>>consumesSendTime = new ConcurrentHashMap<>();


}
