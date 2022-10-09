package com.easy.server.core.entity;

import com.easy.core.entity.MessageId;
import com.easy.core.message.TransmissionMessage;
import com.easy.server.persistenceCollection.FileMapperType;
import com.easy.server.persistenceCollection.JdkSerializer;
import com.easy.server.persistenceCollection.PersistenceSet;
import com.easy.server.persistenceCollection.fileBasedImpl.FilePersistenceArrayList;
import com.easy.server.persistenceCollection.fileBasedImpl.FilePersistenceSet;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class Topic {

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Topic)) return false;
        Topic topic = (Topic) o;
        return Objects.equals(name, topic.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    public static PersistenceSet<String> topics =
            new FilePersistenceSet<>("topics",
                    new JdkSerializer<>(String.class), 10);

    String name;

    HashMap<String, ConsumerGroup> consumerGroups = new HashMap<>();

    /**
     * 检测那些没有回应的消息，如果超过这个时间还没有收到确认应答，则认为丢失了
     */
    static final long messageCheckTimeSeconds = 5;
    static final long redeliverTimedOutMessageIntervalSeconds = 1;

    FilePersistenceArrayList<MessageMetaInfo> persistenceMessageMetaInfo;

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
    final List<TransmissionMessage> messages;

    volatile long size = 0;

    public  Topic(String name) {
        synchronized (Topic.class){
            topics.add(name);
            persistenceMessageMetaInfo =
                    new FilePersistenceArrayList<>(name + "MetaInfo",
                            new JdkSerializer<>(MessageMetaInfo.class),
                            1000,
                            FileMapperType.MergedMemoryMapMapper
                    );

            if (persistenceMessageMetaInfo.isEmpty()) {
                messageMetaInfo = new MessageMetaInfo();
                messageMetaInfo.topicName = name;
            } else {
                messageMetaInfo = persistenceMessageMetaInfo.get(persistenceMessageMetaInfo.size() - 1);
            }
            this.name = name;


            TimeScheduler.executor.scheduleWithFixedDelay(persistenceMessageMetaInfo::compress, 5, 60, TimeUnit.SECONDS);
            TimeScheduler.executor.scheduleWithFixedDelay(this::saveMeta, 1, 1, TimeUnit.SECONDS);


            messages =

                    new FilePersistenceArrayList<>(
                            name + "messages",
                            new JdkSerializer<>(TransmissionMessage.class),
                            1000000,
                            FileMapperType.MergedMemoryMapMapper);


            size = messages.size();
        }
    }


    public void saveMeta() {
        persistenceMessageMetaInfo.add(messageMetaInfo);
    }

    public synchronized void registerConsumerGroup(ConsumerGroup consumerGroup) {

        ConcurrentHashMap<String, Long> consumerPosition = messageMetaInfo.consumerPosition;
        if (!consumerPosition.containsKey(consumerGroup.groupName)) {
            consumerPosition.put(consumerGroup.groupName, 0L);
        }
        consumerGroups.putIfAbsent(consumerGroup.groupName, consumerGroup);
        consumerGroup.initOffsetAsync(this, consumerPosition.get(consumerGroup.groupName));

    }

    /**
     * 表示收到了consumer的回应 这个消息已被消费了
     */
    public void responseReceivedMessage(MessageId messageId, String groupName) {
        ConsumerGroup consumerGroup = consumerGroups.get(groupName);
        if (consumerGroup != null) {
            consumerGroup.commitMessageAsync(this, messageId.getOffset());
        }
    }


    public void putMessage(TransmissionMessage message) {

        final Map<MessageId, Void> setView = receivedMessages;
        setView.put(message.id, null);


        synchronized (messages) {
            message.id.setOffset((long) messages.size());
            messages.add(message);
            size++;
        }


    }

    public TransmissionMessage pullMessage(Long offset) {
        if(!hasNewMessage( offset)){
            return null;
        }
        synchronized (messages) {
            return messages.get(Math.toIntExact(offset));
        }

    }


    public boolean hasNewMessage(Long offset) {
        return offset < size;
    }

    public int totalMessageSize() {

        return Math.toIntExact(size);


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

    public void setConsumerGroupOffset(ConsumerGroup group, Long offset) {

        messageMetaInfo.consumerPosition.put(group.groupName, offset);
    }


}
