package com.easy.server.serverHandler;


import com.easy.core.message.ConsumerInitMessage;
import com.easy.server.EasyServer;
import com.easy.server.core.entity.ConsumerGroup;
import com.easy.server.core.entity.Topic;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 消费者初始化消息处理器 用来注册消费者
 */
@Service
@ChannelHandler.Sharable
public class ConsumerInitMessageHandler extends SimpleChannelInboundHandler<ConsumerInitMessage> {
    public ConsumerInitMessageHandler( @Lazy EasyServer easyServer) {
        this.easyServer = easyServer;
    }

    EasyServer easyServer;
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ConsumerInitMessage msg) throws Exception {
        System.out.println("consumer init for "+msg.getConsumerName());
        final ConcurrentHashMap<String, Topic> topics = easyServer.getTopics();
        final HashMap<String, ConsumerGroup> consumerGroups = easyServer.getConsumerGroups();


        for (String listenedTopic : msg.getListenedTopics()) {
            Topic topic = topics.computeIfAbsent(listenedTopic, Topic::new);
            final ConsumerGroup consumerGroup = consumerGroups.computeIfAbsent(msg.getConsumerGroupName(), ConsumerGroup::new);


            //加入消费者组
            consumerGroup.addOrUpdateConsumer(msg,ctx.channel());

            //确保消费者组在topic中
            topic.registerConsumerGroup(consumerGroup);
        }
      //SimpleChannelInboundHandler会调用一次release 让msg被释放，如果想要往下传 需要手动retain一次
       // ReferenceCountUtil.retain(msg);
    }
}
