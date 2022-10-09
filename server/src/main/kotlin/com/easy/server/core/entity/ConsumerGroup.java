package com.easy.server.core.entity;

import com.easy.core.entity.MessageId;
import com.easy.core.message.ConsumerInitMessage;
import com.easy.core.message.TransmissionMessage;
import io.netty.channel.Channel;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class ConsumerGroup {
    enum MessageState {
        SEND, RECEIVED, UNSENT
    }

    String groupName;
    ConcurrentHashMap<String, Consumer> consumers = new ConcurrentHashMap<>();

    List<String> consumerNames = new ArrayList<>();
    AtomicLong consumerSelector = new AtomicLong();

    /**
     * 消费窗口，一个ConsumerGroup最多有这么多未回应的消息
     */
    private static final Long windowSize = 500L;

    private static final long timeoutMills = 50L;
    private static final long maxTimeoutMills = timeoutMills*5;
    private static final long startWaitingMills = 500L;
    /**
     * 发送窗口
     */
    private final Map<Topic, List<MessageState>> sendWindows = new HashMap<>();
    /**
     * 发送窗口的最左端
     */
    private final Map<Topic, Long> lefts = new HashMap<>();

    public void initOffsetAsync(Topic topic, Long offset) {

        TimeScheduler.runInBindExecutorForce(this,()->{
            ArrayList<MessageState> window = new ArrayList<>();
            for (int i = 0; i < windowSize; i++) {
                window.add(MessageState.UNSENT);
            }
            sendWindows.put(topic, window);
            lefts.put(topic, offset);

            TimeScheduler.runInBindExecutorForce(this, () -> this.waitToStart(topic), 0);
        },0);
    }

    private  boolean inWindow(Topic topic, Long offset) {
        Long left = lefts.get(topic);
        return offset >= left && offset < left + windowSize;
    }

    public  boolean sendMessageToGroup(Topic topic, TransmissionMessage transmissionMessage,long waitMills) {

        final Consumer consumer = nextActiveConsumer();
        //该组没有消费者
        if (consumer == null) {
            return false;
        }
        final MessageId id = transmissionMessage.id;

        if (!inWindow(topic, id.getOffset())) {
            transmissionMessage = topic.pullMessage(lefts.get(topic));
        }

        consumer.putMessage(transmissionMessage);
        startTimeout(topic, transmissionMessage,waitMills);
        return true;
    }

    private int offsetInWindow(Topic topic, Long offset) {
        Long left = lefts.get(topic);
        return (int) (offset - left);
    }

    public  void commitMessageAsync(Topic topic, Long offset) {
        TimeScheduler.runInBindExecutorForce(this,()->{
            Long left = lefts.get(topic);

            final Long oldLeft = left;
            List<MessageState> sendWindow = sendWindows.get(topic);

            if (!inWindow(topic, offset)) {
                return;
            }
            sendWindow.set(offsetInWindow(topic, offset), MessageState.RECEIVED);

            //移动窗口
            while (sendWindow.get(0)==MessageState.RECEIVED) {
                sendWindow.remove(0);
                sendWindow.add(MessageState.UNSENT);
                left++;
            }
            //也许有新的可发送消息
            if(!oldLeft.equals(left)){
                lefts.put(topic, left);
                topic.setConsumerGroupOffset(this, left);

                pullAndSend(topic);
            }
        },0);


    }

    public void waitToStart(Topic topic) {


        if (topic.hasNewMessage(this.lefts.get(topic))) {
            pullAndSend(topic);
        }
            TimeScheduler.runInBindExecutorForce(this, () -> this.waitToStart(topic), startWaitingMills);

    }

    public void pullAndSend(Topic topic) {

        //获取这个topic上可以发送的数量
        Long left = this.lefts.get(topic);
        final int totalMessageSize = topic.totalMessageSize();
        final List<MessageState> window = sendWindows.get(topic);
        //从topic中取出消息 把所有窗口内的未发送消息进行发送
        for (long i = left; i < totalMessageSize && inWindow(topic, i); i++) {
            TransmissionMessage transmissionMessage = topic.pullMessage(i);
            final int index = offsetInWindow(topic, i);
            final MessageState state = window.get(index);

            if (state == MessageState.UNSENT) {
                if (sendMessageToGroup(topic, transmissionMessage,timeoutMills)) {
                    window.set(index, MessageState.SEND);
                } else {

                    break;
                }
            }else {
                break;
            }
        }



    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
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
                    if (value.isActive()) {
                        return value;
                    }
                }
            }
        }

        return null;
    }



    /**
     * 启用一个计时器 用来重发消息
     */
    private void startTimeout(Topic topic, TransmissionMessage transmissionMessage,long waitMills) {



        TimeScheduler.runInBindExecutorForce(this,()->{

            final Long offset = transmissionMessage.id.getOffset();
            //此时可能已经滑出了这个窗口 说明消息已经收到了 所以判断一下
            if (inWindow(topic, offset)) {
                final List<MessageState> messageStates = sendWindows.get(topic);
                final MessageState messageState = messageStates.get(offsetInWindow(topic, transmissionMessage.getId().getOffset()));
                //判断是否收到了
                if(messageState==MessageState.SEND){

                    //双倍等待时间 但是不超过最大值
                    sendMessageToGroup(topic, transmissionMessage,Math.min(waitMills*2,maxTimeoutMills));
                }
            }
        },waitMills);

    }

}
