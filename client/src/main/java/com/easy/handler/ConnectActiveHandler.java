package com.easy.handler;

import com.easy.core.message.ConsumerInitMessage;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.HashSet;
import java.util.Set;
@ChannelHandler.Sharable
public class ConnectActiveHandler extends ChannelInboundHandlerAdapter {
    public Set<String> listenedTopics = new HashSet<>();


    String consumerGroupName;
    String consumerName;

    public ConnectActiveHandler(String consumerGroupName, String consumerName) {
        this.consumerGroupName = consumerGroupName;
        this.consumerName = consumerGroupName +"->"+ consumerName;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        ConsumerInitMessage initMessage = new ConsumerInitMessage(consumerGroupName, consumerName, listenedTopics);


        System.out.println("initMessage+ "+initMessage);
        ctx.writeAndFlush(initMessage);
        super.channelActive(ctx);
    }
}
