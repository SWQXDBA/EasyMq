package com.easy.clientHandler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class ReadableControl extends ChannelInboundHandlerAdapter {
//    //如果不可写 则禁止读
//    @Override
//    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
//        if(!ctx.channel().isWritable()){
//            ctx.channel().config().setAutoRead(false);
//        }
//        super.channelReadComplete(ctx);
//    }

    //
    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        if (ctx.channel().isWritable()) {
            ctx.channel().flush();
            ctx.channel().config().setAutoRead(true);
        }else{
            ctx.channel().config().setAutoRead(false);
        }
        super.channelWritabilityChanged(ctx);
    }
}
