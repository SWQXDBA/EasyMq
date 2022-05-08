package com.easy.server.handler;

import com.easy.core.entity.MessageId;
import com.easy.core.entity.Topic;
import com.easy.core.message.ProducerToServerMessage;
import com.easy.core.message.ProducerToServerMessageUnit;
import com.easy.core.message.TransmissionMessage;
import com.easy.server.EasyServer;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;

@Service
@ChannelHandler.Sharable
public  class ProducerToServerMessageHandler extends SimpleChannelInboundHandler<ProducerToServerMessage> {
    public ProducerToServerMessageHandler(@Lazy EasyServer server) {
        this.server = server;
    }
    EasyServer server;

    private String getProducerNameByAddress(SocketAddress address){
        final InetSocketAddress address1 = (InetSocketAddress) address;
        return "producer: "+address1.getHostString();
    }
    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, ProducerToServerMessage producerToServerMessage) throws Exception {

        for (ProducerToServerMessageUnit message : producerToServerMessage.messages) {
            final String topicName = message.topicName;
            final ConcurrentHashMap<String, Topic> topics = server.getTopics();
            Topic topic = topics.computeIfAbsent(topicName, Topic::new);

            MessageId messageId = new MessageId();
            messageId.setUid(topic.getNextId());
            messageId.setTopicName(topicName);
            String producerName = producerToServerMessage.producerName;
            if(producerName==null){
                producerName = getProducerNameByAddress(channelHandlerContext.channel().remoteAddress());
            }
            messageId.setProducerName(producerName);

            TransmissionMessage transmissionMessage = new TransmissionMessage();
            transmissionMessage.setId(messageId);
            transmissionMessage.setData(message.data);
            topic.putMessage(transmissionMessage);

        }
    }
}