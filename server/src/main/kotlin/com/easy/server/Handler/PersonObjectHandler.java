package com.easy.server.Handler;

import entity.Person;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.SimpleChannelInboundHandler;

import java.io.ObjectInputStream;

public class PersonObjectHandler extends SimpleChannelInboundHandler<Person> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Person msg) throws Exception {
        System.out.println(msg);
    }
}
