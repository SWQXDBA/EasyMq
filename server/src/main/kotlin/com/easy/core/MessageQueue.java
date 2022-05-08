package com.easy.core;

import com.easy.core.message.TransmissionMessage;

import java.util.concurrent.BlockingQueue;

public class MessageQueue {
    BlockingQueue<TransmissionMessage> queue ;
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
}
