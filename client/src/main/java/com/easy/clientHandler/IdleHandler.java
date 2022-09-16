package com.easy.clientHandler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

public class IdleHandler extends ChannelDuplexHandler {
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) evt;
            if (e.state() == IdleState.READER_IDLE) {
                //服务端检测到空闲 关闭channel
                ctx.close();

            } else if (e.state() == IdleState.WRITER_IDLE) {

                final ByteBuf buffer = Unpooled.wrappedBuffer(" ".getBytes());
                //客户端发送心跳包
                ctx.writeAndFlush(buffer);
            }
        }
        super.userEventTriggered(ctx, evt);
    }
}
