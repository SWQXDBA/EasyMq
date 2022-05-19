package com.easy.clientHandler;

import com.easy.core.listener.CallBackListener;
import com.easy.core.listener.DefaultListener;
import com.easy.core.listener.EasyListener;
import com.easy.core.message.CallBackMessage;
import com.easy.core.message.ServerToConsumerMessage;
import com.easy.core.message.TransmissionMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author SWQXDBA
 */
@ChannelHandler.Sharable
public class ServerToConsumerMessageRouter extends SimpleChannelInboundHandler<ServerToConsumerMessage> {
    ObjectMapper objectMapper = new ObjectMapper();
    public Map<String, HashMap<Class<?>, List<EasyListener>>> listeners = new HashMap<>();


    public void addListener(String topicName, EasyListener listener) {

        final HashMap<Class<?>, List<EasyListener>> classListHashMap =  listeners.computeIfAbsent(topicName,(key)-> new HashMap<>(16));

        final List<EasyListener> defaultListeners =  classListHashMap.computeIfAbsent(listener.messageType, (key)->new LinkedList<>());

        defaultListeners.add(listener);
    }


    public ServerToConsumerMessageRouter() {

        objectMapper.registerModule(new JavaTimeModule());
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ServerToConsumerMessage serverToConsumerMessage) throws Exception {

        for (TransmissionMessage message : serverToConsumerMessage.getMessages()) {

            final String topicName = message.getTopicName();
            if (!listeners.containsKey(topicName)) {
                continue;
            }
            final Class<?> dataClass = message.getDataClass();
            //根据topic找到监听的listener
            final HashMap<Class<?>, List<EasyListener>> classListHashMap = listeners.get(topicName);
            if (classListHashMap == null) {
                return;
            }
            final List<EasyListener> listeners = classListHashMap.get(dataClass);
            if (listeners == null || listeners.size() == 0) {
                return;
            }
            //找到这个listener接收的消息Class

            final Object userData = objectMapper.readValue(message.data, dataClass);
            for (EasyListener listener : listeners) {
                if(listener instanceof DefaultListener){
                    ((DefaultListener)listener).handle(message.getId(), userData);
                }else if(listener instanceof CallBackListener){
                    final Object callBack = ((CallBackListener) listener).answer(message.getId(), userData);
                    sendCallBack(ctx,message,callBack);
                }
            }
        }
        //super.channelRead(ctx, msg);
    }

    private void sendCallBack(ChannelHandlerContext ctx,TransmissionMessage message ,Object callBack){
        CallBackMessage callBackMessage = new CallBackMessage();
        callBackMessage.setAnswerTo(message.getId());
        callBackMessage.setCallBackDataClass(callBack.getClass());
        try {
            callBackMessage.setData(objectMapper.writeValueAsBytes(callBack));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        ctx.channel().writeAndFlush(callBackMessage);
    }
}
