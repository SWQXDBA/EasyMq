package com.easy.test;

import com.easy.EasyClient;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class MultProducer {
    public static void main(String[] args) {
        final ExecutorService executorService = Executors.newFixedThreadPool(100);
        for (int k = 0; k < 4; k++) {
            int finalK1 = k;
            executorService.execute(()->{
                EasyClient client = new EasyClient(8081, "localhost", null,null);

                client.addNode(8081, "localhost");
                final ExecutorService service = Executors.newFixedThreadPool(1000);

                AtomicBoolean stop = new AtomicBoolean(false);
                int finalK = finalK1;
                service.execute(() -> {
                    while (true) {
                        if (!stop.get()) {
                            for (int i = 0; i < 1; i++) {

                                if (stop.get()) {
                                    return;
                                }
                                for (int j = 0; j < 8000; j++) {

                                    client.sendToTopic("strstrstr", "topic"+ finalK);
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
            });

        }

        try {
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
