package com.easy.core.entity;

import com.easy.core.message.TransmissionMessage;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Topic {


    String name;
    List<MessageQueue> logicQueues = new ArrayList<>();
    HashMap<String, ConsumerGroup> consumerGroups = new HashMap<>();

    /**
     * 储存一些可丢失的信息，比如消息已经被哪些消费者组消费过
     */
    MessageMetaInfo messageMetaInfo;

    /**
     * 这个topic上的所有消息
     */
    ConcurrentHashMap<MessageId, TransmissionMessage> messages = new ConcurrentHashMap<>();


    AtomicInteger queueSelector = new AtomicInteger();

    int resendSeconds;

    public Topic(String name) {
        messageMetaInfo = new MessageMetaInfo();
        messageMetaInfo.topicName = name;
        this.name = name;
        for (int i = 0; i < 20; i++) {
            logicQueues.add(new MessageQueue(this));
        }
    }

    public void registerConsumerGroup(ConsumerGroup consumerGroup){
        consumerGroups.putIfAbsent(consumerGroup.groupName,consumerGroup);
    }

    //表示收到了consumer的回应 这个消息已被消费了
    public void responseReceivedMessage(MessageId messageId,String groupName){
        final Set<ConsumerGroup> unconsumedGroups = messageMetaInfo.unconsumedGroups.get(messageId);
        if(unconsumedGroups==null){
            return;
        }
        final ConsumerGroup consumerGroup = consumerGroups.get(groupName);
        if(consumerGroup==null){
            return;
        }
        unconsumedGroups.remove(consumerGroup);
        if (unconsumedGroups.isEmpty()){
            messageMetaInfo.unconsumedGroups.remove(messageId);
            messageMetaInfo.consumesSendTime.remove(messageId);
            messages.remove(messageId);
        }

    }


    public long getNextId() {
        return queueSelector.incrementAndGet();
    }



    public void sendMessage(MessageId messageId){
        sendMessage(messages.get(messageId));
    }
    public void sendMessage(TransmissionMessage message){
        //记录消息元信息
        messageMetaInfo.consumesSendTime.put(message.id,new HashMap<>());
        messageMetaInfo.unconsumedGroups.put(message.id,new HashSet<>());

        for (ConsumerGroup consumerGroup : consumerGroups.values()) {
            sendMessageToGroup(consumerGroup,message);
        }
    }


    private void sendMessageToGroup(ConsumerGroup consumerGroup, TransmissionMessage transmissionMessage) {

        final Consumer consumer = consumerGroup.nextConsumer();
        //该组没有消费者
        if(consumer==null){
            return;
        }
        //记录消息往这一组投递的时间
        final HashMap<ConsumerGroup, LocalDateTime> consumerGroupLocalDateTimeHashMap = messageMetaInfo.consumesSendTime.get(transmissionMessage.id);
        consumerGroupLocalDateTimeHashMap.put(consumerGroup,LocalDateTime.now());
        //记录未响应状态
        final Set<ConsumerGroup> consumerGroups = messageMetaInfo.unconsumedGroups.get(transmissionMessage.id);
        consumerGroups.add(consumerGroup);

        consumer.putMessage(transmissionMessage);


    }


    public boolean containsMessage(MessageId messageId){
        return  messages.containsKey(messageId);
    }

    public void putMessage(TransmissionMessage message) {
        final Set<MessageId> setView = messageMetaInfo.receivedMessages;
        setView.add(message.id);

        if (messages.containsKey(message.id)) {
            return;
        }
        messages.put(message.id,message);
        enAnyQueue(message);
    }

    private void enAnyQueue(TransmissionMessage transmissionMessage) {
        final int num = queueSelector.getAndIncrement()%logicQueues.size();
        logicQueues.get(num).store(transmissionMessage);
    }


}
