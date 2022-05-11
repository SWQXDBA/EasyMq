package com.easy.server.serverHandler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.concurrent.atomic.AtomicLong;

public class InboundSpeedTestHandler extends ChannelInboundHandlerAdapter {
    AtomicLong atomicLong = new AtomicLong();
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        System.out.println(atomicLong.incrementAndGet());
        super.channelRead(ctx, msg);
    }
}
