package com.easy.server.core.entity;

import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class TimeScheduler {
    public static ScheduledExecutorService executor = Executors.newScheduledThreadPool(12, new ThreadFactory() {
        int i = 0;
        @Override
        public Thread newThread(@NotNull Runnable r) {
            i++;
            return  new Thread(r,"TimeScheduler"+i);
        }
    });


    public static EventExecutorGroup executorGroup = new DefaultEventExecutorGroup(12, new ThreadFactory() {
        int i = 0;
        @Override
        public Thread newThread(@NotNull Runnable r) {
            i++;
            return new Thread(r, "TimeScheduler-executor-"+i);
        }
    });
    private static  ConcurrentHashMap<Object, EventExecutor> bindExecutors = new ConcurrentHashMap<>();

    static AtomicLong count = new AtomicLong();
    static{
         new Thread(new Runnable() {
            @Override
            public void run() {
                while(true){
                    try {
                        Thread.sleep(1000);
                        System.out.println(count+"::runInBindExecutorForce");
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
    }
    public static void runInBindExecutorForce(Object me,Runnable runnable,long delayMills){

        final EventExecutor eventExecutor = bindExecutors.computeIfAbsent(me, (o -> executorGroup.next()));
        count.getAndIncrement();

        if(delayMills==0){
            eventExecutor.execute(()->{
                count.getAndDecrement();
                try{
                    runnable.run();
                }catch (Exception e){
                    log.warn("execute failed , will try it again on next time ");
                    e.printStackTrace();
                    runInBindExecutorForce(me,runnable,0);
                }
            });
        }else{
            eventExecutor.schedule(()->{
                count.getAndDecrement();
                try{
                    runnable.run();
                }catch (Exception e){
                    log.warn("execute failed , will try it again on next time ");
                    e.printStackTrace();
                    runInBindExecutorForce(me,runnable,Math.max(delayMills,100));
                }
            },delayMills, TimeUnit.MILLISECONDS);
        }

    }
    public static void runInBindExecutor(Object me,Runnable runnable,long delayMills){
        final EventExecutor eventExecutor = bindExecutors.computeIfAbsent(me, (o -> executorGroup.next()));
        eventExecutor.schedule(runnable,delayMills, TimeUnit.MILLISECONDS);
    }

}
