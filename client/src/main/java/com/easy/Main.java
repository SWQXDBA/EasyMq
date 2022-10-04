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

        EasyClient client = new EasyClient(8081, "localhost", null,null);

        client.addNode(8081, "localhost");
        final ExecutorService service = Executors.newFixedThreadPool(1000);

        AtomicBoolean stop = new AtomicBoolean(false);
        service.execute(() -> {
            while (true) {
                if (!stop.get()) {
                    for (int i = 0; i < 10; i++) {
                        service.execute(() -> {
                            if (stop.get()) {
                                return;
                            }
                            for (int j = 0; j < 300; j++) {
                                client.sendToTopic("str", "topic");
                            }
                        });
                    }
                }
                try {
                    Thread.sleep(200);
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
