package com.easy.clientHandler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

import java.util.concurrent.atomic.AtomicLong;
@ChannelHandler.Sharable
public class DataOutboundCounter extends ChannelOutboundHandlerAdapter {
    public volatile AtomicLong atomicLong = new AtomicLong();

    public DataOutboundCounter() {
        new Thread(()->{
            long current = 0;
            while (true) {
                System.out.println("ChannelOutboundHandlerAdapter "+atomicLong.get());
                System.out.println("outbound speed "+(atomicLong.get() -current)/1024/1024+"mbps");
                current = atomicLong.get();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        final ByteBuf buf = (ByteBuf) msg;
        final int i = buf.readableBytes();
        atomicLong.getAndAdd(i);
        super.write(ctx, msg, promise);
    }
}
