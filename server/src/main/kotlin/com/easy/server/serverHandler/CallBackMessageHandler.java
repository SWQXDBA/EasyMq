package com.easy.server.serverHandler;

import com.easy.core.entity.MessageId;
import com.easy.core.message.CallBackMessage;
import com.easy.server.EasyServer;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;


/**
 * 把消费者的回调消息传给发送方
 */
@Service
@ChannelHandler.Sharable
public class CallBackMessageHandler extends SimpleChannelInboundHandler<CallBackMessage> {

    final EasyServer server;

    ConcurrentHashMap<MessageId, Channel> callBacks = new ConcurrentHashMap<>();

    public CallBackMessageHandler(@Lazy EasyServer server) {
        this.server = server;
    }

    public void addCallBack(MessageId messageId, Channel producer) {
        callBacks.put(messageId, producer);
    }


    @Override
    protected void channelRead0(ChannelHandlerContext ctx, CallBackMessage msg) throws Exception {
        if(callBacks.containsKey(msg.getAnswerTo())){
            callBacks.get(msg.getAnswerTo()).writeAndFlush(msg);
            callBacks.remove(msg.getAnswerTo());
        }
    }
}
