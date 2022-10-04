package com.easy;

import com.easy.core.entity.MessageId;
import com.easy.core.listener.DefaultListener;

import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ConsumerMain {
    public static void main(String[] args) {
        AtomicLong atomicLong = new AtomicLong();

        EasyClient client = new EasyClient(8081, "localhost", "group1", "消费者1");
        client.addListener(new DefaultListener<String>("topic") {
            @Override
            public void handle(MessageId messageId, String message) {
                atomicLong.getAndIncrement();

                client.confirmationResponse(messageId);
            }
        });

        client.addNode(8081, "localhost");
        final ExecutorService service = Executors.newFixedThreadPool(1000);


        AtomicBoolean stop = new AtomicBoolean(false);


        service.execute(() -> {
            long l = 0;
            while (true) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println(atomicLong.get() - l);
                l = atomicLong.get();
            }


        });


        client.run();
    }
}
