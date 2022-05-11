package com.easy.server.serverHandler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

import java.util.concurrent.atomic.AtomicLong;

public class OutboundSpeedTestHandler extends ChannelOutboundHandlerAdapter {
    AtomicLong flushCount = new AtomicLong();
    @Override
    public void flush(ChannelHandlerContext ctx) throws Exception {
        System.out.println("flushCount "+flushCount.incrementAndGet());
        super.flush(ctx);
    }
    AtomicLong writeCount = new AtomicLong();
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        System.out.println("writeCount "+flushCount.incrementAndGet());
        super.write(ctx, msg, promise);
    }
}
