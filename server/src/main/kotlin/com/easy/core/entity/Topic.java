package com.easy.core.entity;

import com.easy.core.message.TransmissionMessage;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class Topic {
    public Topic(String name) {
        this.name = name;
    }

    String name;
    Set<MessageQueue> logicQueues = new HashSet<>();
    HashMap<String, ConsumerGroup> consumerGroups = new HashMap<>();

    /**
     * 储存一些可丢失的信息，比如消息已经被哪些消费者组消费过
     */
    MessageMetaInfo messageMetaInfo = new MessageMetaInfo();

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
    ConcurrentHashMap<MessageId, TransmissionMessage> consumingMessages = new ConcurrentHashMap<>();

    AtomicLong idGenerator = new AtomicLong();

    int resendSeconds;

    public void flushConsumingMessages() {
        final Iterator<Map.Entry<MessageId, TransmissionMessage>> iterator = consumingMessages.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<MessageId, TransmissionMessage> longMessageEntry = iterator.next();
            final TransmissionMessage transmissionMessage = longMessageEntry.getValue();


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


    /**
     * 当消费者失活后，准备把这个消息重新投递给同一个消费者组的可能是其它消费者，这个方法只是进行一些准备
     * 具体重新发送是在flushConsumingMessages的时候再执行。
     */
    public void sendMessageToGroup(ConsumerGroup consumerGroup, TransmissionMessage transmissionMessage) {


    }

    public void putMessage(TransmissionMessage message) {

        ObjectMapper mapper = new ObjectMapper();
        try {
            final Person person = mapper.readValue(message.data, Person.class);
            System.out.println(person.name);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(message);
        for (ConsumerGroup consumerGroup : consumerGroups.values()) {

        }
        //     coreTransmissionMessage.id = getNextId();
        message.data = message.data;
        enAnyQueue(message);
    }

    private void enAnyQueue(TransmissionMessage transmissionMessage) {
        //todo 通过负载算法找出一个队列塞进去
    }

    private boolean isMessageConsumed(MessageId messageId) {

        final Set<ConsumerGroup> consumedGroups = messageMetaInfo.unconsumedGroups.get(messageId);
        if (consumedGroups == null) {
            return true;
        }
        return consumedGroups.size() == 0;
    }
}
