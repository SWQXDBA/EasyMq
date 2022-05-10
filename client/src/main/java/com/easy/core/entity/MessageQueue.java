package com.easy.core.entity;

import com.easy.core.message.TransmissionMessage;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class MessageQueue {
    BlockingQueue<TransmissionMessage> queue = new LinkedBlockingQueue<>();
    Topic topic;
    Thread takeThread;
    /**
     * 往队列中投递一个消息，理论上不会阻塞
     */
    public void store(TransmissionMessage transmissionMessage){
        try {
            queue.put(transmissionMessage);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public MessageQueue(Topic topic) {
        takeThread = new Thread(this::work);
        takeThread.start();
        this.topic = topic;
    }


    /**
     * 取出一条消息 可能阻塞
     */
    public TransmissionMessage take(){
        try {
            return queue.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    private void work(){
        while(true){
            final TransmissionMessage message = take();
            System.out.println("堆积消息:"+queue.size());
            topic.consumingMessages.add(message.id);
            topic.sendMessage(message);
        }
    }
}
