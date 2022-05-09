package com.easy.core.entity;

import com.easy.core.message.TransmissionMessage;


import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class Topic {
    public Topic(String name) {
        messageMetaInfo = new MessageMetaInfo();
        messageMetaInfo.topicName = name;
        this.name = name;

        logicQueues.add(new MessageQueue(this));
    }

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

    /**
     * <消息.id,消息>
     * 正在被若干个消费者组消费中的消息，
     * 请注意，只要有任何一个消费者完成了拒绝应答，那么这个消息与那个消费者对应的consumesVersion会被重新分配，对应的consumeTypes会被设置为UNCONSUMED(未消费)
     * <p>
     * 服务器应该在合适的时机把consuming中
     * <p>
     * 超时重发策略，服务端会尝试让同一个消费者消费同一条消息，这样可以在网络原因丢失消息的情况下不重复消费。
     * (每个Consumer客户端维持着一个自己的Set<id>)
     * 考虑到一个问题 如果某个consumer挂了 那么服务端将无法知道是否被消费了
     * 此时服务端会重新向任意一个同组内的consumer发送这个消息，此时可能造成重复消费，这个要让客户端自己进行避免重复消费。
     */
    Set<MessageId> consumingMessages = ConcurrentHashMap.newKeySet();

    AtomicLong idGenerator = new AtomicLong();

    int resendSeconds;


    public void registerConsumerGroup(ConsumerGroup consumerGroup){
        consumerGroups.putIfAbsent(consumerGroup.groupName,consumerGroup);

    }

    public void flushConsumingMessages() {
        final Iterator<MessageId> iterator = consumingMessages.iterator();
        while (iterator.hasNext()) {


            //todo 可能要把它移除
/*
            if (transmissionMessage.isConsumed()) {
                iterator.remove();
                continue;
            }
*/
/*
            for (Map.Entry<ConsumerGroup, ConsumeType> consumeTypeEntry : messageMetaInfo.consumeTypes.get(transmissionMessage.id).entrySet()) {
                final ConsumerGroup consumerGroup = consumeTypeEntry.getKey();

                final ConsumeType consumeType = consumeTypeEntry.getValue();
                if (consumeType == ConsumeType.CONSUMING) {
                    final LocalDateTime sendTime = messageMetaInfo.consumesSendTime.get(transmissionMessage.id).get(consumerGroup);
                    final LocalDateTime timeout = sendTime.plus(resendSeconds, ChronoUnit.SECONDS);
                    final LocalDateTime now = LocalDateTime.now();
                    if (now.isAfter(timeout)) {
                        sendMessageToGroup(consumerGroup,transmissionMessage);
                    }
                } else if (consumeType == ConsumeType.UNCONSUMED) {

                }
                continue;

            }*/
        }
    }

    public long getNextId() {
        return idGenerator.incrementAndGet();
    }



    public void sendMessage(MessageId messageId){
        sendMessage(messages.get(messageId));
    }
    public void sendMessage(TransmissionMessage message){

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
        logicQueues.get(0).store(transmissionMessage);
    }

    private boolean isMessageConsumed(MessageId messageId) {

        final Set<ConsumerGroup> consumedGroups = messageMetaInfo.unconsumedGroups.get(messageId);
        if (consumedGroups == null) {
            return true;
        }
        return consumedGroups.size() == 0;
    }
}
