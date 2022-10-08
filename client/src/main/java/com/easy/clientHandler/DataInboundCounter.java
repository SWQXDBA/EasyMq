package com.easy.clientHandler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.concurrent.atomic.AtomicLong;
@ChannelHandler.Sharable
public class DataInboundCounter extends ChannelInboundHandlerAdapter {
   public volatile AtomicLong atomicLong = new AtomicLong();

    public DataInboundCounter() {
        new Thread(()->{
            long current = 0;
            while (true) {

                System.out.println("ChannelInboundHandlerAdapter "+atomicLong.get());
                System.out.println("inbound speed "+(atomicLong.get() -current)/1024/1024+"mbps");
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
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        final ByteBuf buf = (ByteBuf) msg;

        final int i =  buf.readableBytes();
        atomicLong.getAndAdd(i);

        super.channelRead(ctx, msg);

        //
        // 如果作为最后一个channel 要把msg消耗掉 就是说不要用super.channelRead继续传递下去，否则会warn，
        // 但是要手动释放引用计数 ReferenceCountUtil.release(msg) 否则会内存泄漏
//        ReferenceCountUtil.release(msg);
////        super.channelRead(ctx, msg);
    }
}
