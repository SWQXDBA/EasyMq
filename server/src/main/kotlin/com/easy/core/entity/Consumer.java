package com.easy.core.entity;

import com.easy.core.Client;
import com.easy.core.message.ServerToConsumerMessage;
import com.easy.core.message.TransmissionMessage;

import java.time.LocalDateTime;

public class Consumer extends Client {

    static int passedTimeSecond = 30;
    public ConsumerGroup group;

    public LocalDateTime lastResponseTime;

    public Boolean isAlive() {
        final LocalDateTime now = LocalDateTime.now();
        final LocalDateTime expirationTime = lastResponseTime.plusSeconds(passedTimeSecond);
        return now.isBefore(expirationTime);
    }

    /**
     * 如果每有一条消息 就进行一次发送未免太浪费了 这里可以积攒一些消息再进行发送，同时可以塞入一些其他信息。
     */
    private transient volatile ServerToConsumerMessage currentMessage;


    /**
     * 立刻发送消息
     * @param transmissionMessage
     */
    public void sendImmediately(String topicName,TransmissionMessage transmissionMessage) {
        getCurrentMessage().putMessage(topicName,transmissionMessage);
        doSend();
    }

    /**
     * 判断当前是否需要发送消息
     *
     * @return
     */
    private boolean needToSend() {
        return true;
    }

    /**
     * 给这个consumer递送一条消息  但不一定立即发送
     *
     * @param transmissionMessage
     */
    public synchronized void putMessage(String topicName,TransmissionMessage transmissionMessage) {
        getCurrentMessage().putMessage(topicName,transmissionMessage);
        if (needToSend()) {
            doSend();
        }
    }

    /**
     * 马上发送当前要发送的消息
     */
    public synchronized void doSend() {

        if (currentMessage == null) {
            return;
        }
        System.out.println(currentMessage);
        //todo send currentMessage

    }

    private synchronized ServerToConsumerMessage getCurrentMessage() {
        if (currentMessage == null) {
            currentMessage = new ServerToConsumerMessage();
        }
        return currentMessage;
    }


}