package com.easy.core;

import com.easy.core.enums.ConsumeType;
import com.easy.core.message.Message;
import com.easy.core.message.ProducerToServerMessageUnit;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class Topic {
    String name;
    List<MessageQueue> queues;
    List<ConsumerGroup> consumerGroups;
    /**
     * <消息.id,消息>
     * 正在被若干个消费者组消费中的消息，
     * 请注意，只要有任何一个消费者完成了拒绝应答，那么这个消息与那个消费者对应的consumesVersion会被重新分配，对应的consumeTypes会被设置为UNCONSUMED(未消费)
     *
     * 服务器应该在合适的时机把consuming中
     * UNCONSUMED的消息发送给对应的消费者组，然后把consumeTypes置为CONSUMING (重新发送)
     * CONSUMING的消息发送给对应的消费者
     * 如果所有consumeTypes均为 CONSUMED 表示该消息已经被彻底消费完毕!!!此时考虑将其移出consuming。
     *
     * 超时重发策略，服务端会尝试让同一个消费者消费同一条消息，这样可以在网络原因丢失消息的情况下不重复消费。
     * (每个Consumer客户端维持着一个自己的Set<id>)
     * 考虑到一个问题 如果某个consumer挂了 那么服务端将无法知道是否被消费了
     * 此时服务端会重新向任意一个同组内的consumer发送这个消息，此时可能造成重复消费，这个要让客户端自己进行避免重复消费。
     */
    HashMap<Long, Message> consumingMessages;
    AtomicLong idGenerator;

    int resendSeconds;
    public void flushConsumingMessages() {
        final Iterator<Map.Entry<Long, Message>> iterator = consumingMessages.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, Message> longMessageEntry = iterator.next();
            final Message message = longMessageEntry.getValue();

            if (message.isConsumed()) {
                //todo 可能要把它移除
                iterator.remove();
                continue;
            }

            for (Map.Entry<Consumer, ConsumeType> consumeTypeEntry : message.consumeTypes.entrySet()) {
                final Consumer consumer = consumeTypeEntry.getKey();
                final ConsumeType consumeType = consumeTypeEntry.getValue();
                if (consumeType == ConsumeType.CONSUMING) {
                    final LocalDateTime sendTime = message.consumesSendTime.get(consumer);
                    final LocalDateTime timeout = sendTime.plus(resendSeconds, ChronoUnit.SECONDS);
                    final LocalDateTime now = LocalDateTime.now();
                    if(now.isAfter(timeout)){
                        if(consumer.isAlive()){
                            //todo 超时重发机制
                        }
                    }
                }else if(consumeType == ConsumeType.UNCONSUMED){

                }
                continue;

            }
        }
    }

    public long getNextId() {
        return idGenerator.incrementAndGet();
    }


    /**
     * 当消费者失活后，准备把这个消息重新投递给同一个消费者组的可能是其它消费者，这个方法只是进行一些准备
     * 具体重新发送是在flushConsumingMessages的时候再执行。
     */
    public void resendToAnyConsumer(Consumer consumer, Message message) {
        message.consumesSendTime.remove(consumer);
        message.consumeTypes.remove(consumer);
        consumer = consumer.group.nextConsumer();
        message.consumeTypes.put(consumer,ConsumeType.UNCONSUMED);
    }
    public void putMessage(ProducerToServerMessageUnit message){
        Message coreMessage = new Message();
        for (ConsumerGroup consumerGroup : consumerGroups) {
            final Consumer consumer = consumerGroup.nextConsumer();
            coreMessage.consumeTypes.put(consumer,ConsumeType.UNCONSUMED);
        }
        coreMessage.id = getNextId();
        coreMessage.data = message.data;
        enAnyQueue(coreMessage);
    }
    private void enAnyQueue(Message message){
     //todo 通过负载算法找出一个队列塞进去
    }
}
