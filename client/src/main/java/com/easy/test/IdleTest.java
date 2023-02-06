package com.easy.test;

import com.easy.EasyClient;

public class IdleTest {
    public static void main(String[] args) {

        EasyClient client = new EasyClient(8081, "localhost", "group1", "消费者1");


        client.run();
    }
}
