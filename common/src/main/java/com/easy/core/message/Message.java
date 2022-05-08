package com.easy.core.message;

import com.easy.core.Consumer;
import com.easy.core.enums.ConsumeType;

import java.time.LocalDateTime;
import java.util.HashMap;

/**
 * 在消息队列中真正储存的消息
 * 注意 每个Consumer都应该属于不同的ConsumerGroup
 */
public class Message {
    /**
     * Topic下的全局id 只会生成一次
     */
    public Long id;

    /**
     * 真正要传递的数据
     */
    public byte[] data;
    /**
     * consumeType中CONSUMED的数量 如果等于consumeTypes.size那么说明所有消费者组都已经完成了该消息的消费了
     */
    public int consumedCount;

    /**
     * 用于记录所有消费者类别对该消息的消费状态
     * <消费类名,状态枚举>
     */
    public HashMap<Consumer, ConsumeType> consumeTypes;


    /**
     * 消息发送时间 用于决定是否执行try-refuse 还是继续等待
     */
    public HashMap<Consumer, LocalDateTime> consumesSendTime;

    /**
     * 这个消息是否已经被所有消费者消费完毕
     */
    public boolean isConsumed() {
        return consumedCount == consumeTypes.size();
    }
}
