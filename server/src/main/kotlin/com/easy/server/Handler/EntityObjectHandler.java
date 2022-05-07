package com.easy.server.Handler;

import entity.Entity;
import entity.Person;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class EntityObjectHandler extends SimpleChannelInboundHandler<Entity> {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Entity msg) throws Exception {
        System.out.println(msg);
    }
}
