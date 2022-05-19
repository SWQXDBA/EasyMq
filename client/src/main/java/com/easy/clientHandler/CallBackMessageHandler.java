package com.easy.clientHandler;

import com.easy.core.entity.MessageId;
import com.easy.core.message.CallBackMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * 处理那些有回调的消息
 */
public class CallBackMessageHandler extends SimpleChannelInboundHandler<CallBackMessage> {
    ObjectMapper objectMapper = new ObjectMapper();

    ConcurrentHashMap<MessageId, Consumer<?>> callBacks = new ConcurrentHashMap<>();
    public void addCallBack(MessageId messageId, Consumer<?> callBack){
        callBacks.put(messageId,callBack);
    }

    public CallBackMessageHandler() {

        objectMapper.registerModule(new JavaTimeModule());
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, CallBackMessage msg) throws Exception {
        final MessageId answerTo = msg.getAnswerTo();
        if(callBacks.containsKey(answerTo)){
            final Consumer consumer = callBacks.get(answerTo);
            final Object answer = objectMapper.readValue(msg.data, msg.getCallBackDataClass());
            try {
                consumer.accept(answer);
                callBacks.remove(answerTo);
            }catch (java.lang.ClassCastException e){
                System.out.println("回调类型转换错误!");
                System.out.println(answer);
                e.printStackTrace();
            }


        }
    }
}
