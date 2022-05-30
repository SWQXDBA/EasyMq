package com.easy.server.core.entity;

import com.easy.core.message.ConsumerInitMessage;
import io.netty.channel.Channel;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class ConsumerGroup {
    String groupName;
    ConcurrentHashMap<String, Consumer> consumers = new ConcurrentHashMap<>();

    List<String> consumerNames = new ArrayList<>();
    AtomicLong consumerSelector = new AtomicLong();

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
            consumerNames.add(consumer.consumerName);
        }

    }

    /**
     * 获取下一个消费者
     *
     * @return
     */
    public Consumer nextActiveConsumer() {
        final int index = (int) (consumerSelector.getAndIncrement() % consumerNames.size());
        final String consumerName = consumerNames.get(index);
        final Consumer consumer = consumers.getOrDefault(consumerName, null);
        if (consumer != null) {
            if (consumer.isActive()) {
                return consumer;
            } else {
                for (Consumer value : consumers.values()) {
                    if(value.isActive()){
                        return value;
                    }
                }
            }
        }
        return null;
    }
}
