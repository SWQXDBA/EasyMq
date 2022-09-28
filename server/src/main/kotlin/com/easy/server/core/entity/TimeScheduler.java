package com.easy.server.core.entity;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

public class TimeScheduler {
    public static ScheduledExecutorService executor = Executors.newScheduledThreadPool(4, new ThreadFactory() {
        int i = 0;
        @Override
        public Thread newThread(@NotNull Runnable r) {
            i++;
            return  new Thread(r,"TimeScheduler"+i);
        }
    });
}
