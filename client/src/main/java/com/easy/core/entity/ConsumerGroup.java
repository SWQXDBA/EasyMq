package com.easy.core.entity;

import com.easy.core.message.ConsumerInitMessage;
import io.netty.channel.Channel;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class ConsumerGroup {
    String groupName;
    ConcurrentHashMap<String,Consumer> consumers = new ConcurrentHashMap<>();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConsumerGroup that = (ConsumerGroup) o;
        return Objects.equals(groupName, that.groupName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupName);
    }

    public ConsumerGroup(String groupName) {
        this.groupName = groupName;
    }
    public void addConsumer(ConsumerInitMessage  message, Channel channel){
        Consumer consumer = new Consumer(message.getConsumerName(),this,channel);
        consumers.put(consumer.consumerName,consumer);
    }

    /**
     * 获取下一个消费者
     * @return
     */
    public Consumer nextConsumer(){
        for (Consumer value : consumers.values()) {
            return value;
        }
        return null;
    }
}
