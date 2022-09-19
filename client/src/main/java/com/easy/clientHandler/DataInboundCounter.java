package com.easy.clientHandler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.*;

import java.util.concurrent.atomic.AtomicLong;
@ChannelHandler.Sharable
public class DataInboundCounter extends ChannelInboundHandlerAdapter {
   public volatile AtomicLong atomicLong = new AtomicLong();

    public DataInboundCounter() {
        new Thread(()->{
            while (true) {
                System.out.println("ChannelInboundHandlerAdapter "+atomicLong.get());
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

    }




    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        final ByteBuf buf = (ByteBuf) msg;

        final int i =  buf.readableBytes();
        atomicLong.getAndAdd(i);
        super.channelRead(ctx, msg);
    }
}