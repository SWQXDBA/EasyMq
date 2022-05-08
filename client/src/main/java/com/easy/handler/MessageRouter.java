package com.easy.handler;

import com.easy.EasyListener;
import com.easy.core.message.ServerToConsumerMessage;
import com.easy.core.message.ServerToConsumerMessageUnit;
import com.easy.core.message.TransmissionMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.HashMap;
import java.util.Map;

/**
 * @author SWQXDBA
 */
@ChannelHandler.Sharable

public class MessageRouter extends ChannelInboundHandlerAdapter {
    ObjectMapper objectMapper = new ObjectMapper();
    public Map<String, EasyListener> listeners = new HashMap<>();
    public Map<EasyListener, Class> messageClassMap = new HashMap<>();

    public void addListener(String topicName, EasyListener listener) {
        listeners.put(topicName, listener);
        messageClassMap.put(listener,listener.messageClass);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        final ServerToConsumerMessage serverToConsumerMessage = (ServerToConsumerMessage) msg;
        for (ServerToConsumerMessageUnit messageUnit : serverToConsumerMessage.getMessages()) {
            final String topicName = messageUnit.getTopicName();
            if(!listeners.containsKey(topicName)){
                continue;
            }
            //根据topic找到监听的listener
            final EasyListener listener = listeners.get(topicName);
            //找到这个listener接收的消息Class
            final Class aClass = messageClassMap.get(listener);
            final TransmissionMessage transmissionMessage = messageUnit.getMessage();
            final Object userData = objectMapper.readValue(transmissionMessage.data, aClass);
            listener.handle(transmissionMessage.getId(),userData);
        }
        super.channelRead(ctx, msg);
    }
}
