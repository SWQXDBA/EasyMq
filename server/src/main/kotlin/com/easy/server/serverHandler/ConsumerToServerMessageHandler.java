package com.easy.server.serverHandler;

import com.easy.core.entity.MessageId;
import com.easy.core.message.ConsumerToServerMessage;
import com.easy.server.EasyServer;
import com.easy.server.core.entity.Topic;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

/**
 * 处理消费者的ack消息
 */
@Service
@ChannelHandler.Sharable
public class ConsumerToServerMessageHandler extends SimpleChannelInboundHandler<ConsumerToServerMessage> {
    @Autowired
    @Lazy
    EasyServer server;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ConsumerToServerMessage msg) throws Exception {
        for (MessageId messageId : msg.confirmationResponse) {
            final Topic topic = server.getTopics().get(messageId.getTopicName());

            if (topic.responseReceivedMessage(messageId,msg.consumerGroupName)) {
                //持久层删除消息
                server.getPersistenceProvider().remove(messageId);
            }

        }

    }
}
