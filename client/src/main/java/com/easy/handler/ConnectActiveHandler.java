package com.easy.handler;

import com.easy.core.message.ConsumerInitMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.*;

public class ConnectActiveHandler extends ChannelInboundHandlerAdapter {
   public Set<String> listenedTopics = new HashSet<>();
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        String consumerName ="consumer: "+ ctx.channel().localAddress().toString();
        ConsumerInitMessage initMessage = new ConsumerInitMessage(consumerName,listenedTopics);
        ctx.writeAndFlush(initMessage);
        super.channelActive(ctx);
    }
}
