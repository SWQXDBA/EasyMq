package com.easy.server.Handler


import com.easy.core.entity.MessageId
import com.easy.core.entity.Topic

import com.easy.core.message.ProducerToServerMessage
import com.easy.core.message.TransmissionMessage
import com.easy.server.EasyServer
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.springframework.context.annotation.Lazy
import org.springframework.stereotype.Service
import java.net.InetSocketAddress
import java.net.SocketAddress


@Service
@Sharable
class ProducerToServerMessageHandler(@Lazy var server: EasyServer) :
    SimpleChannelInboundHandler<ProducerToServerMessage>() {

    fun getProducerNameByAddress(address: SocketAddress): String? {
        val address1 = address as InetSocketAddress
        return "producer: " + address1.hostString
    }
    @Throws(Exception::class)
    override fun channelRead0(
        channelHandlerContext: ChannelHandlerContext,
        producerToServerMessage: ProducerToServerMessage
    ) {
        GlobalScope.launch {
            for (messageUnit in producerToServerMessage.messages) {
                val topicName = messageUnit.topicName
                val topics = server.topics
                val topic = topics.computeIfAbsent(topicName) { name: String? -> Topic(name) }

                //生成messageId
                val messageId = MessageId()

                messageId.topicName = topicName
                var producerName = producerToServerMessage.producerName
                if (producerName == null) {
                    producerName = getProducerNameByAddress(channelHandlerContext.channel().remoteAddress())
                }
                messageId.producerName = producerName
                messageId.uid = producerName+"=> "+messageUnit.messageProductionId

                //先确认一下是否收到过
                // 但是确认和后面的插入不是原子的 可能会在确认完成后被其他线程插入，导致持久化了两次，
                //可能性极低，出现这种情况一般认为是客户端重复发送了相同的消息（id也是相同的）
                if (topic.containsMessage(messageId)) {
                    continue
                }


                val transmissionMessage = TransmissionMessage(messageId, messageUnit.data, messageUnit.dataClass,topicName)

                server.localPersistenceProvider.save(transmissionMessage)

                topic.putMessage(transmissionMessage)


            }
            channelHandlerContext.fireChannelRead(producerToServerMessage)
        }

    }
}