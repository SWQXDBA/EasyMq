package com.easy;

import com.easy.core.listener.DefaultListener;
import com.easy.core.entity.MessageId;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class Main2 {
    public static void main(String[] args) {
        AtomicLong atomicLong = new AtomicLong();


        EasyClient client = new EasyClient(8080, "localhost", "group2", "消费者2");
        client.addListener(new DefaultListener<String>("topic") {
            @Override
            public void handle(MessageId messageId, String message) {
                atomicLong.getAndIncrement();
                client.confirmationResponse(messageId);
                //   System.out.println(messageId.getUid());
            }
        });
        final ExecutorService service = Executors.newFixedThreadPool(10);
        service.execute(() -> {
            while (true) {
                long last = atomicLong.get();
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println(atomicLong.get()-last+"   已收到"+atomicLong+"/"+client.getSentMessage());
            }
        });


        client.run();
    }
}
