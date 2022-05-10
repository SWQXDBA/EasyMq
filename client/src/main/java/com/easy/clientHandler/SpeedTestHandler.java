package com.easy.clientHandler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.concurrent.atomic.AtomicLong;

public class SpeedTestHandler extends ChannelInboundHandlerAdapter {
    AtomicLong cnt = new AtomicLong();
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        System.out.println("channelRead "+ cnt.incrementAndGet());
        super.channelRead(ctx, msg);
    }
}
