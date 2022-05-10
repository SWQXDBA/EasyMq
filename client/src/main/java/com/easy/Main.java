package com.easy;

import com.easy.core.entity.MessageId;

import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author SWQXDBA
 */
public class Main {

    public static void main(String[] args) {
        AtomicLong atomicLong = new AtomicLong();
        EasyClient client = new EasyClient(8080,"localhost","group1","消费者1");
        client.addListener(new EasyListener<Date>("topic") {
            @Override
            public void handle(MessageId messageId, Date message) {
                System.out.println(messageId.getUid());
            }
        });

        client.run();
    }
}
