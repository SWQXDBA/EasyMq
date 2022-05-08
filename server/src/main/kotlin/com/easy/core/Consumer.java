package com.easy.core;

import com.easy.core.message.ServerToConsumerMessage;
import com.easy.core.message.TransmissionMessage;

import java.time.LocalDateTime;
import java.util.Objects;

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
    public void sendImmediately(TransmissionMessage transmissionMessage) {
        getCurrentMessage().putMessage(transmissionMessage);
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
    public synchronized void putMessage(TransmissionMessage transmissionMessage) {
        getCurrentMessage().putMessage(transmissionMessage);
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
        //todo send currentMessage

    }

    private synchronized ServerToConsumerMessage getCurrentMessage() {
        if (currentMessage == null) {
            currentMessage = new ServerToConsumerMessage();
        }
        return currentMessage;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Consumer consumer = (Consumer) o;
        return Objects.equals(name, consumer.name) && Objects.equals(ip, consumer.ip) && Objects.equals(port, consumer.port);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, ip, port);
    }
}
