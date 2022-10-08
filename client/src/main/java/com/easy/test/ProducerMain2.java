package com.easy.test;

import com.easy.EasyClient;

import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ProducerMain2 {
    public static void main(String[] args) {
        AtomicLong atomicLong = new AtomicLong();

        EasyClient client = new EasyClient(8081, "localhost", null,null);

        client.addNode(8081, "localhost");
        final ExecutorService service = Executors.newFixedThreadPool(10);

        AtomicBoolean stop = new AtomicBoolean(false);
        service.execute(() -> {
            while (true) {
                if (!stop.get()) {
                    for (int i = 0; i < 1; i++) {

                        if (stop.get()) {
                            return;
                        }
                        for (int j = 0; j < 15000; j++) {

                            client.sendToTopic("str", "topic2");
                        }

                    }
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });




        service.execute(() -> {
            while (true) {
                Scanner scanner = new Scanner(System.in);
                scanner.nextLine();
                stop.set(!stop.get());
            }
        });

        client.run();
    }
}
