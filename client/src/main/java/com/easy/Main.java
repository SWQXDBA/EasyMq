package com.easy;

import com.easy.core.entity.MessageId;

/**
 * @author SWQXDBA
 */
public class Main {

    public static void main(String[] args) {

        EasyClient client = new EasyClient(8080,"localhost","group1","消费者1");
        client.addListener( new EasyListener<String>("topic") {
            @Override
            public void handle(MessageId messageId, String message) {
                System.out.println("收到了"+message);
            }
        });

        client.run();
    }
}
