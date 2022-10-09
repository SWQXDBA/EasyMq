package com.easy.test;

import com.easy.EasyClient;
import com.easy.core.entity.MessageId;
import com.easy.core.listener.DefaultListener;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class MultConsumer {
    public static void main(String[] args) {
        AtomicLong atomicLong = new AtomicLong();
        final ExecutorService executorService = Executors.newFixedThreadPool(100);
        for (int i = 0; i < 4; i++) {
            int finalI = i;
            executorService.execute(()->{
                EasyClient client = new EasyClient(8081, "localhost", "group"+finalI, "消费者"+finalI);
                client.addListener(new DefaultListener<String>("topic"+ finalI) {
                    @Override
                    public void handle(MessageId messageId, String message) {
                        atomicLong.getAndIncrement();

                        client.confirmationResponse(messageId);
                    }
                });

                client.addNode(8081, "localhost");
                final ExecutorService service = Executors.newFixedThreadPool(10);


                AtomicBoolean stop = new AtomicBoolean(false);





                client.run();
            });

        }
        executorService.execute(() -> {
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
        try {
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
