package com.easy.core;

import com.easy.core.message.Message;

import java.util.concurrent.BlockingQueue;

public class MessageQueue {
    BlockingQueue<Message> queue ;
    /**
     * 往队列中投递一个消息，理论上不会阻塞
     */
    public void store(Message message){
        try {
            queue.put(message);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 取出一条消息 可能阻塞
     */
    public Message take(){
        try {
            return queue.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }
}
