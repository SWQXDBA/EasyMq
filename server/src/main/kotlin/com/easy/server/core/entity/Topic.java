package com.easy.server.core.entity;

import com.easy.core.entity.MessageId;
import com.easy.core.message.TransmissionMessage;
import com.easy.server.persistenceCollection.FileMapperType;


import com.easy.server.persistenceCollection.JdkSerializer;
import com.easy.server.persistenceCollection.PersistenceSet;
import com.easy.server.persistenceCollection.fileBasedImpl.FilePersistenceArrayList;
import com.easy.server.persistenceCollection.fileBasedImpl.FilePersistenceMap;
import com.easy.server.persistenceCollection.fileBasedImpl.FilePersistenceSet;
import org.springframework.cglib.core.WeakCacheKey;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Topic {

    public static PersistenceSet<String> topics =
            new FilePersistenceSet<>("topics",
            new JdkSerializer<>(String.class),10);

    String name;
    List<MessageQueue> logicQueues = new ArrayList<>();
    HashMap<String, ConsumerGroup> consumerGroups = new HashMap<>();

    /**
     * 检测那些没有回应的消息，如果超过这个时间还没有收到确认应答，则认为丢失了
     */
    static final long messageCheckTimeSeconds = 5;
    static final long redeliverTimedOutMessageIntervalSeconds = 1;

    List<MessageMetaInfo> persistenceMessageMetaInfo;

    /**
     * 储存一些可丢失的信息，比如消息已经被哪些消费者组消费过
     */
    MessageMetaInfo messageMetaInfo;

    /**
     * 用来告诉producer 此消息已经被服务器接收过
     */
    Map<MessageId, Void> receivedMessages = Collections.synchronizedMap(new WeakHashMap<>());


    /**
     * 这个topic上的所有消息
     */
    Map<MessageId, TransmissionMessage> messages ;


    AtomicInteger queueSelector = new AtomicInteger();

    public Topic(String name) {
        topics.add(name);
        persistenceMessageMetaInfo = Collections.synchronizedList(
                new FilePersistenceArrayList<>(name + "MetaInfo",
                        new JdkSerializer<>(MessageMetaInfo.class),
                        1000,
                        FileMapperType.MemoryMapMapper
                )
        );

        if (persistenceMessageMetaInfo.isEmpty()) {
            messageMetaInfo = new MessageMetaInfo();
            messageMetaInfo.topicName = name;
        } else {
            messageMetaInfo = persistenceMessageMetaInfo.get(persistenceMessageMetaInfo.size() - 1);
        }
        this.name = name;
        for (int i = 0; i < 20; i++) {
            logicQueues.add(new MessageQueue(this));
        }

        TimeScheduler.executor.scheduleWithFixedDelay(this::redeliverTimedOutMessage, 1, redeliverTimedOutMessageIntervalSeconds, TimeUnit.SECONDS);
        TimeScheduler.executor.scheduleWithFixedDelay(this::saveMeta, 1, 5, TimeUnit.SECONDS);

        messages =Collections.synchronizedMap(new FilePersistenceMap<>(
                name + "TopicMessages",
                new JdkSerializer<>(MessageId.class),
                new JdkSerializer<>(TransmissionMessage.class),

                1000,
                -1f,
                FileMapperType.MemoryMapMapper)
        );
    }


    private void saveMeta() {
        persistenceMessageMetaInfo.add(messageMetaInfo);
    }

    /**
     * 检测哪些消息一直没有回应，考虑重投
     */
    public  void redeliverTimedOutMessage() {

        for (Map.Entry<MessageId, ConcurrentHashMap<String, LocalDateTime>> messageEntry : messageMetaInfo.consumesSendTime.entrySet()) {
            final ConcurrentHashMap<String, LocalDateTime> value = messageEntry.getValue();
            final MessageId messageId = messageEntry.getKey();
            for (Map.Entry<String, LocalDateTime> timeEntry : value.entrySet()) {

                final LocalDateTime sendTime = timeEntry.getValue();
                final LocalDateTime now = LocalDateTime.now();
                if (sendTime.plus(messageCheckTimeSeconds, ChronoUnit.SECONDS).isBefore(now)) {
                    String consumerGroupName =  timeEntry.getKey();
                    final ConsumerGroup consumerGroup = consumerGroups.get(consumerGroupName);
                    if(consumerGroup==null){
                        return;
                    }
                    final TransmissionMessage message = messages.get(messageId);
                    //这个过程中 可能已经接收到回复了 就不需要再次投递了
                    if (message != null) {
                        sendMessageToGroup(consumerGroup, message);
                    }

                }
            }
        }
    }

    public void registerConsumerGroup(ConsumerGroup consumerGroup) {
        consumerGroups.putIfAbsent(consumerGroup.groupName, consumerGroup);
    }

    /**
     * 表示收到了consumer的回应 这个消息已被消费了
     *
     * @return 是否消息被所有消费者组消费完成
     */
    public  boolean responseReceivedMessage(MessageId messageId, String groupName) {
        final ConcurrentHashMap<String , LocalDateTime> map = messageMetaInfo.consumesSendTime.get(messageId);
        final ConsumerGroup consumerGroup = consumerGroups.get(groupName);
        if (consumerGroup == null) {
            return true;
        }
        map.remove(groupName);

        //消息已被全部消费者组消费完成，可以丢弃
        if (map.isEmpty()) {
            messages.remove(messageId);
            return true;
        }
        return false;
    }

    public long getNextId() {
        return queueSelector.incrementAndGet();
    }


    public void sendMessage(MessageId messageId) {
        sendMessage(messages.get(messageId));
    }

    public void sendMessage(TransmissionMessage message) {
        //记录消息元信息
        messageMetaInfo.consumesSendTime.put(message.id, new ConcurrentHashMap<>());

        for (ConsumerGroup consumerGroup : consumerGroups.values()) {
            sendMessageToGroup(consumerGroup, message);
        }
    }


    private void sendMessageToGroup(ConsumerGroup consumerGroup, TransmissionMessage transmissionMessage) {
        final Consumer consumer = consumerGroup.nextActiveConsumer();
        //该组没有消费者
        if (consumer == null) {
            return;
        }
        final MessageId id = transmissionMessage.id;
        //记录消息往这一组投递的时间
        ConcurrentHashMap<String, LocalDateTime> consumerGroupLocalDateTimeHashMap = messageMetaInfo.consumesSendTime.getOrDefault(id, null);
        //高并发时可能存在准备重投时，这个消息刚刚好被响应 此时就不用重新投递了
        if (consumerGroupLocalDateTimeHashMap == null) {
            return;
        }
        consumerGroupLocalDateTimeHashMap.put(consumerGroup.groupName, LocalDateTime.now());

        consumer.putMessage(transmissionMessage);
    }


    public  boolean containsMessage(MessageId messageId) {
        return messages.containsKey(messageId);
    }

    public  void putMessage(TransmissionMessage message) {

        final Map<MessageId, Void> setView = receivedMessages;
        setView.put(message.id, null);

        if (messages.containsKey(message.id)) {
            return;
        }
        messages.put(message.id, message);
        enAnyQueue(message);
    }

    /**
     * 暂时实现： 在确认应答后即时删除
     * 理论上应该进行策略保留以供生产者查询
     *
     * @param messageId
     */
    public void confirmAnswerToProducer(MessageId messageId) {
        receivedMessages.remove(messageId);
    }

    private void enAnyQueue(TransmissionMessage transmissionMessage) {
        final int num = queueSelector.getAndIncrement() % logicQueues.size();
        logicQueues.get(num).store(transmissionMessage);
    }


}
