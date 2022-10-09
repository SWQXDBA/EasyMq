package com.easy.server



import com.easy.clientHandler.DataInboundCounter
import com.easy.clientHandler.IdleHandler
import com.easy.clientHandler.ReadableControl
import com.easy.core.entity.MessageId
import com.easy.server.core.entity.ConsumerGroup
import com.easy.server.core.entity.Topic

import com.easy.server.serverHandler.*
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.*
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.serialization.ClassResolvers
import io.netty.handler.codec.serialization.ObjectDecoder
import io.netty.handler.codec.serialization.ObjectEncoder
import io.netty.handler.timeout.IdleStateHandler
import io.netty.util.concurrent.DefaultEventExecutorGroup
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

@Service
class EasyServer(
    @Value("\${server.port}")
    val port: Int, val producerToServerMessageHandler: ProducerToServerMessageHandler,
    private val consumerInitMessageHandler: ConsumerInitMessageHandler,
    private val consumerToServerMessageHandler:ConsumerToServerMessageHandler,
    private val callBackMessageHandler: CallBackMessageHandler,

) {

    companion object{
        var INSTANSE :EasyServer? = null
    }

    private val dataInboundCounter: DataInboundCounter = DataInboundCounter()

    val topics = ConcurrentHashMap<String, Topic>();

    val consumerGroups = ConcurrentHashMap<String, ConsumerGroup>()


    init {
        Companion.INSTANSE = this
        Topic.topics.forEach {
            topics[it] = Topic(it)
        }
    }


    fun run() {

        val serverBootstrap = ServerBootstrap()
        val bossGroup = NioEventLoopGroup()
        val workGroup = NioEventLoopGroup()


        try {
            serverBootstrap.group(bossGroup,workGroup)
                .channel(NioServerSocketChannel::class.java)
                .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK,WriteBufferWaterMark(1024*1024*4,1024*1024*16))
                .childHandler(object : ChannelInitializer<SocketChannel>() {
                    override fun initChannel(ch: SocketChannel?) {
                        ch!!.pipeline()
                            //服务器5秒内没有接收到可读请求则触发事件
                            .addLast(IdleStateHandler(10,0,0,TimeUnit.SECONDS))
                            //心跳包检测
                            .addLast(IdleHandler())
                            .addLast(dataInboundCounter)
                            .addLast(ReadableControl())
//                            .addLast(LoggingHandler(LogLevel.INFO))
                            .addLast(
                                ObjectDecoder(
                                    Int.MAX_VALUE,
                                    ClassResolvers.weakCachingConcurrentResolver(this::class.java.classLoader)
                                )
                            )
                            .addLast(ObjectEncoder())



                            .addLast(consumerInitMessageHandler)
                            .addLast(producerToServerMessageHandler)
                            .addLast(consumerToServerMessageHandler)
                            .addLast(callBackMessageHandler)

                    }

                })

            val channelFuture = serverBootstrap.bind(port)
            channelFuture.sync()


            channelFuture.channel().closeFuture().sync()

        } catch (e: Exception) {

        }
    }

    fun listenCallBackMessage(messageId: MessageId,producer: Channel){
        callBackMessageHandler.addCallBack(messageId,producer);
    }
}