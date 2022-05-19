package com.easy.server.core.entity;

import com.easy.core.message.ConsumerInitMessage;
import io.netty.channel.Channel;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class ConsumerGroup {
    String groupName;
    ConcurrentHashMap<String, Consumer> consumers = new ConcurrentHashMap<>();

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

    public void addOrUpdateConsumer(ConsumerInitMessage message, Channel channel) {
        if (consumers.containsKey(message.getConsumerName())) {
            final Consumer consumer = consumers.get(message.getConsumerName());
            consumer.resetChannel(channel);

        } else {
            Consumer consumer = new Consumer(message.getConsumerName(), this, channel);
            consumers.put(consumer.consumerName, consumer);
        }

    }

    /**
     * 获取下一个消费者
     *
     * @return
     */
    public Consumer nextConsumer() {
        for (Consumer consumer : consumers.values()) {
            return consumer;
        }
        return null;
    }
}
