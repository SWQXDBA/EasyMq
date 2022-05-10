package com.easy;

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

        AtomicLong sentMessageCount = new AtomicLong();
        EasyClient client = new EasyClient(8080, "localhost", "group1", "消费者1");
        client.addListener(new EasyListener<String>("topic") {
            @Override
            public void handle(MessageId messageId, String message) {
                atomicLong.getAndIncrement();
                client.confirmationResponse(messageId);
                //   System.out.println(messageId.getUid());
            }
        });

        final ExecutorService service = Executors.newFixedThreadPool(1000);
        service.execute(() -> {
            while (true) {
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println(atomicLong+" "+sentMessageCount);
            }
        });


        AtomicBoolean stop = new AtomicBoolean();
        service.execute(() -> {
            while (!stop.get()){

                for (int i = 0; i < 100000; i++) {
                    service.execute(() -> {
                        sentMessageCount.getAndIncrement();
                        client.sendToTopic("str", "topic");
                    });
                }
                try {
                    Thread.sleep(2000);
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
