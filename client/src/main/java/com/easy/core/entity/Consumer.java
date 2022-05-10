package com.easy.core.entity;

import com.easy.core.Client;
import com.easy.core.message.ServerToConsumerMessage;
import com.easy.core.message.TransmissionMessage;
import io.netty.channel.Channel;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Consumer extends Client {

    static int passedTimeSecond = 30;

    static int longestSendIntervalMills = 100;

    public LocalDateTime lastSendTime = LocalDateTime.now();

    public String consumerName;
    public ConsumerGroup group;
    public LocalDateTime lastResponseTime = LocalDateTime.now();

    final ExecutorService service = Executors.newSingleThreadExecutor();

    private Channel channel;

    public Consumer(String consumerName, ConsumerGroup group, Channel channel) {
        this.consumerName = consumerName;
        this.group = group;
        this.channel = channel;

        service.execute(() -> {
            while (true) {
                try {
                    Thread.sleep(longestSendIntervalMills);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                doSend();
            }

        });

    }

    public Boolean isAlive() {
        final LocalDateTime now = LocalDateTime.now();
        final LocalDateTime expirationTime = lastResponseTime.plusSeconds(passedTimeSecond);
        return now.isBefore(expirationTime);
    }

    /**
     * 如果每有一条消息 就进行一次发送未免太浪费了 这里可以积攒一些消息再进行发送
     */
    private transient volatile ServerToConsumerMessage currentMessage = new ServerToConsumerMessage();


    /**
     * 立刻发送消息
     *
     * @param transmissionMessage
     */
    public void sendImmediately(TransmissionMessage transmissionMessage) {
        currentMessage.putMessage(transmissionMessage);
        doSend();
    }

    /**
     * 判断当前是否需要发送消息
     *
     * @return
     */
    private boolean needToSend() {
        LocalDateTime now = LocalDateTime.now();
        if (lastSendTime.plus(longestSendIntervalMills, ChronoUnit.MILLIS).isBefore(now)) {
            return true;
        }
        return false;
    }

    /**
     * 给这个consumer递送一条消息  但不一定立即发送
     *
     * @param transmissionMessage
     */
    public void putMessage(TransmissionMessage transmissionMessage) {
        currentMessage.putMessage(transmissionMessage);
        if (needToSend()) {
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
            if (this.currentMessage.getMessages().isEmpty()) {
                return;
            }
            sendMessage = this.currentMessage;
            this.currentMessage = new ServerToConsumerMessage();

        }
        channel.writeAndFlush(sendMessage);
        lastSendTime = LocalDateTime.now();

    }


}
