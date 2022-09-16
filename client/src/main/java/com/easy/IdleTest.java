package com.easy;

import com.easy.EasyClient;

import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class IdleTest {
    public static void main(String[] args) {

        EasyClient client = new EasyClient(8081, "localhost", "group1", "消费者1");


        client.run();
    }
}
