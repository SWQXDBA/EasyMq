package com.easy.clientHandler;

import com.easy.EasyListener;
import com.easy.core.message.ServerToConsumerMessage;
import com.easy.core.message.TransmissionMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author SWQXDBA
 */
@ChannelHandler.Sharable
public class MessageRouter extends ChannelInboundHandlerAdapter {
    ObjectMapper objectMapper = new ObjectMapper();
    public Map<String, HashMap<Class<?>, List<EasyListener<?>>>> listeners = new HashMap<>();


    public void addListener(String topicName, EasyListener<?> listener) {

        final HashMap<Class<?>, List<EasyListener<?>>> classListHashMap =  listeners.computeIfAbsent(topicName,(key)-> new HashMap<>(16));
        final List<EasyListener<?>> easyListeners =  classListHashMap.computeIfAbsent(listener.messageType, (key)->new LinkedList<>());

        easyListeners.add(listener);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {



        final ServerToConsumerMessage serverToConsumerMessage = (ServerToConsumerMessage) msg;

        for (TransmissionMessage message : serverToConsumerMessage.getMessages()) {

            final String topicName = message.getTopicName();
            if (!listeners.containsKey(topicName)) {
                continue;
            }
            final Class<?> dataClass = message.getDataClass();
            //根据topic找到监听的listener
            final HashMap<Class<?>, List<EasyListener<?>>> classListHashMap = listeners.get(topicName);
            if (classListHashMap == null) {
                return;
            }
            final List<EasyListener<?>> easyListeners = classListHashMap.get(dataClass);
            if (easyListeners == null || easyListeners.size() == 0) {
                return;
            }
            //找到这个listener接收的消息Class

            final Object userData = objectMapper.readValue(message.data, dataClass);
            for (EasyListener listener : easyListeners) {
                listener.handle(message.getId(), userData);
            }
        }
        //super.channelRead(ctx, msg);
    }
}
