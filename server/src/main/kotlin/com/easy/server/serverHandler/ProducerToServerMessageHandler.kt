package com.easy.server.serverHandler


import com.easy.core.message.ProducerToServerMessage
import com.easy.core.message.ServerToProducerMessage
import com.easy.core.message.TransmissionMessage
import com.easy.server.EasyServer
import com.easy.server.core.entity.Topic
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.springframework.context.annotation.Lazy
import org.springframework.stereotype.Service
import java.net.InetSocketAddress
import java.net.SocketAddress

/**
 * 用来处理生产者投递的消息
 */
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
                launch flag@{
                    //生成messageId
                    val messageId = messageUnit.messageId
                    val topicName = messageId.topicName
                    val topics = server.topics
                    val topic = topics.computeIfAbsent(topicName) { name: String? -> Topic(name) }


                    //先确认一下是否收到过
                    // 但是确认和后面的插入不是原子的 可能会在确认完成后被其他线程插入，导致持久化了两次，
                    //可能性极低，出现这种情况一般认为是客户端重复发送了相同的消息（id也是相同的）
                    if (topic.containsMessage(messageId)) {
                        return@flag
                    }


                    val transmissionMessage = TransmissionMessage(
                        messageId,
                        messageUnit.data,
                        messageUnit.dataClass,
                        topicName,
                        messageUnit.callBack
                    )



                    if (messageUnit.callBack){
                        server.listenCallBackMessage(messageId,channelHandlerContext.channel());
                    }
                    topic.putMessage(transmissionMessage)

                    val serverToProducerMessage = ServerToProducerMessage()
                    serverToProducerMessage.receivedIds.add(messageId)
                    channelHandlerContext.channel().writeAndFlush(serverToProducerMessage)
                    topic.confirmAnswerToProducer(messageId)


                }
            }

        }
        channelHandlerContext.fireChannelRead(producerToServerMessage)


    }
}