package com.easy.server.core.entity;

import com.easy.core.Client;
import com.easy.core.message.ServerToConsumerMessage;
import com.easy.core.message.TransmissionMessage;
import io.netty.channel.Channel;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;

/**
 * 表示建立了连接的一个消费者 用来发送消息
 */
public class Consumer extends Client {

    static int passedTimeSecond = 30;

    static int longestSendIntervalMills = 1000;

    public String consumerName;
    public ConsumerGroup group;
    public LocalDateTime lastResponseTime = LocalDateTime.now();



    //一次性最多批量发送多少数据 如果数据太大 序列化会出问题
    final static int BatchSendBytesSize = 1024 * 1024;
    private Channel channel;



    public void resetChannel(Channel channel) {
        this.channel = channel;
    }

    public boolean isActive() {
        return channel.isActive();
    }

    public Consumer(String consumerName, ConsumerGroup group, Channel channel) {
        this.consumerName = consumerName;
        this.group = group;
        this.channel = channel;

        TimeScheduler.executor
                .scheduleWithFixedDelay(this::checkAndSend,
                        1,longestSendIntervalMills,
                        TimeUnit.MILLISECONDS);

    }


    /**
     * 如果每有一条消息 就进行一次发送未免太浪费了 这里可以积攒一些消息再进行发送
     */
    private transient volatile ServerToConsumerMessage cacheMessage = new ServerToConsumerMessage();


    /**
     * 立刻发送消息 除非channel.isActive()==false
     *
     * @param transmissionMessage
     */
    public void sendImmediately(TransmissionMessage transmissionMessage) {
        cacheMessage.putMessage(transmissionMessage);
        if (channel.isActive()) {
            doSend();
        }
    }


    /**
     * 给这个consumer递送一条消息  但不一定立即发送
     *
     * @param transmissionMessage
     */
    public void putMessage(TransmissionMessage transmissionMessage) {

        System.out.println("put");
        if (transmissionMessage.isNeedCallBack()) {
            sendImmediately(transmissionMessage);
        } else {
            cacheMessage.putMessage(transmissionMessage);
            checkAndSend();
        }
    }

    private void checkAndSend() {
        if (channel.isActive() && channel.isWritable()) {
            doSend();
        }
    }

    /**
     * 马上发送当前要发送的消息
     * synchronized为了避免send多次
     */
    private void doSend() {

        ServerToConsumerMessage sendMessage;
        synchronized (this) {

            if (this.cacheMessage.getMessages().isEmpty()) {

                return;
            }
            sendMessage = this.cuttingMessage();
        }
        if (channel.isActive()) {
            //注意 在这个过程中 如果说客户端断开连接 那么这一部分消息会丢失掉，需要重新发送给consumerGroup
            channel.writeAndFlush(sendMessage);
        }

    }

    /**
     * 确保单次发送的数据不会过多
     *
     */
    private ServerToConsumerMessage cuttingMessage() {
        final ServerToConsumerMessage currentMessage = this.cacheMessage;
        List<TransmissionMessage> sortList = new ArrayList<>(currentMessage.getMessages());
        sortList.sort(Comparator.comparingInt((TransmissionMessage m) -> m.getData().length));
        ServerToConsumerMessage serverToConsumerMessage = new ServerToConsumerMessage();

        int currentSize = 0;
        for (TransmissionMessage transmissionMessage : sortList) {
            currentSize += transmissionMessage.getData().length;
            //检测大小，但是最少发送一条消息
            if (currentSize < BatchSendBytesSize || serverToConsumerMessage.getMessages().isEmpty()) {
                serverToConsumerMessage.putMessage(transmissionMessage);
                currentMessage.getMessages().remove(transmissionMessage);
            } else {
                break;
            }
        }
        return serverToConsumerMessage;

    }

}
