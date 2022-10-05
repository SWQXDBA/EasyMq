package com.easy.server.core.entity;

import com.easy.core.entity.MessageId;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

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
@AllArgsConstructor
@NoArgsConstructor
@Data
public class MessageMetaInfo  implements Serializable {
    private static final long serialVersionUID = 1L;

    String topicName;

    /**
     * 消费者在队列上的消费位置
     */
    public ConcurrentHashMap<String,Long> consumerPosition = new ConcurrentHashMap<>();

}
