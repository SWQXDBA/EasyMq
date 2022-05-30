package com.easy;

import com.easy.core.listener.DefaultListener;
import com.easy.core.entity.MessageId;

import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author SWQXDBA
 */
public class Main {

    public static void main(String[] args) {
        AtomicLong atomicLong = new AtomicLong();

        EasyClient client = new EasyClient(8080, "localhost", "group1", "消费者1");
        client.addListener(new DefaultListener<String>("topic") {
            @Override
            public void handle(MessageId messageId, String message) {
                atomicLong.getAndIncrement();
             //   System.out.println("   已收到"+atomicLong+"/"+client.getSentMessage());
                client.confirmationResponse(messageId);
            }
        });


        final ExecutorService service = Executors.newFixedThreadPool(1000);
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


        AtomicBoolean stop = new AtomicBoolean();
        service.execute(() -> {
            while (!stop.get()){
                for (int i = 0; i < 100; i++) {
                    service.execute(() -> {
                        for (int j = 0; j < 1000; j++) {
                            client.sendToTopic("str", "topic");
                        }
                    });
                }
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }


        });

        service.execute(()->{
                Scanner scanner = new Scanner(System.in);
            scanner.nextLine();
            stop.set(true);
        });

        client.run();
    }
}
