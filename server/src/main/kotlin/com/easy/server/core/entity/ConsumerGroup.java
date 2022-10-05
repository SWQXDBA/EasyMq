package com.easy.server.core.entity;

import com.easy.core.entity.MessageId;
import com.easy.core.message.ConsumerInitMessage;
import com.easy.core.message.TransmissionMessage;
import io.netty.channel.Channel;

import java.sql.Time;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class ConsumerGroup {
    String groupName;
    ConcurrentHashMap<String, Consumer> consumers = new ConcurrentHashMap<>();

    List<String> consumerNames = new ArrayList<>();
    AtomicLong consumerSelector = new AtomicLong();

    /**
     * 消费窗口，一个ConsumerGroup最多有这么多未回应的消息
     */
    private static final Long windowSize = 1000L;
    /**
     * 发送窗口
     */
    private final Map<Topic,List<Boolean>> sendWindows = new HashMap<>();
    /**
     * 发送窗口的最左端
     */
    private final Map<Topic,Long> lefts = new HashMap<>();


    private synchronized boolean inWindow(Topic topic,Long offset){
        Long left = lefts.get(topic);
        return offset>=left&&offset<left+ windowSize;
    }
    public void sendMessageToGroup(Topic topic, TransmissionMessage transmissionMessage) {
        if(!sendWindows.containsKey(topic)){
            LinkedList<Boolean> window = new LinkedList<>();
            for (int i = 0; i < windowSize; i++) {
                window.add(false);
            }
            sendWindows.put(topic,window);
        }


        final Consumer consumer = nextActiveConsumer();
        //该组没有消费者
        if (consumer == null) {
            return;
        }
        final MessageId id = transmissionMessage.id;
        if(!lefts.containsKey(topic)){
            lefts.put(topic,id.getOffset());
        }

        if(!inWindow(topic,id.getOffset())){
            return;
        }

        consumer.putMessage(transmissionMessage);
    }
    private int offsetInWindow(Topic topic,Long offset){
        Long left = lefts.get(topic);
        return (int) (offset-left);
    }
    public  void commitMessage(Topic topic,Long offset){
        Long left = lefts.get(topic);
        long oldRight = left+ windowSize;

        List<Boolean> sendWindow = sendWindows.get(topic);
        synchronized(this){
            if(!inWindow(topic,offset)){
                return;
            }
            sendWindow.set(offsetInWindow(topic,offset),true);

            //移动窗口
            while(sendWindow.get(0)){
                sendWindow.remove(0);
                sendWindow.add(false);
                left++;
            }
            lefts.put(topic,left);

        }
        //根据新的窗口发送消息
        for (long i = oldRight; i < left+ windowSize; i++) {
            TransmissionMessage transmissionMessage = topic.pullMessage(i);
            sendMessageToGroup(topic,transmissionMessage);
        }




    }


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
     */
    private Consumer nextActiveConsumer() {
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

    private void startWatch(Topic topic,Long offset){
        LocalDateTime now = LocalDateTime.now();
        TimeScheduler.executor.schedule(()->{
            if(inWindow(topic,offset)){
                return;
            }
        },1, TimeUnit.NANOSECONDS); 
    }

}
