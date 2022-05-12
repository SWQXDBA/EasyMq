package com.easy.core.entity;

import com.easy.core.message.TransmissionMessage;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class Topic {


    String name;
    List<MessageQueue> logicQueues = new ArrayList<>();
    HashMap<String, ConsumerGroup> consumerGroups = new HashMap<>();

    /**
     * 检测那些没有回应的消息，如果超过这个时间还没有收到确认应答，则认为丢失了
     */
    static final  long messageCheckTimeSeconds = 10;
    static final long redeliverTimedOutMessageIntervalSeconds = 1;
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
        Executors.newSingleThreadExecutor().execute(()->{
            while(true){
                try {
                    Thread.sleep(redeliverTimedOutMessageIntervalSeconds*1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                redeliverTimedOutMessage();
            }
        });

    }

    /**
     * 检测哪些消息一直没有回应，考虑重投
     */
    public void redeliverTimedOutMessage(){

        for (Map.Entry<MessageId, HashMap<ConsumerGroup, LocalDateTime>> messageEntry : messageMetaInfo.consumesSendTime.entrySet()) {
            final HashMap<ConsumerGroup, LocalDateTime> value = messageEntry.getValue();
            final MessageId messageId = messageEntry.getKey();
            for (Map.Entry<ConsumerGroup, LocalDateTime> timeEntry : value.entrySet()) {

                final LocalDateTime sendTime = timeEntry.getValue();
                final LocalDateTime now = LocalDateTime.now();
                if(sendTime.plus(messageCheckTimeSeconds, ChronoUnit.SECONDS).isBefore(now)){
                    final ConsumerGroup consumerGroup = timeEntry.getKey();
                    final TransmissionMessage message = messages.get(messageId);
                    sendMessageToGroup(consumerGroup,message);
                }
            }
        }
    }

    public void registerConsumerGroup(ConsumerGroup consumerGroup){
        consumerGroups.putIfAbsent(consumerGroup.groupName,consumerGroup);
    }

    //表示收到了consumer的回应 这个消息已被消费了
    public void responseReceivedMessage(MessageId messageId,String groupName){
        final HashMap<ConsumerGroup, LocalDateTime> map = messageMetaInfo.consumesSendTime.get(messageId);
        final ConsumerGroup consumerGroup = consumerGroups.get(groupName);
        if(consumerGroup==null){
            return;
        }
        map.remove(consumerGroup);

        if (map.isEmpty()){
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
