package com.easy.clientHandler;

import com.easy.EasyClient;
import com.easy.core.message.ServerToProducerMessage;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
@ChannelHandler.Sharable
public class ServerToProducerMessageHandler extends SimpleChannelInboundHandler<ServerToProducerMessage> {
    EasyClient client;

    public ServerToProducerMessageHandler(EasyClient client) {
        this.client = client;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ServerToProducerMessage msg) throws Exception {
        msg.receivedIds.forEach((messageId)->{
            client.confirmationResponse(messageId);
        });
    }
}
