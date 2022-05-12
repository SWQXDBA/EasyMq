package com.easy.server


import com.easy.core.entity.Consumer
import com.easy.core.entity.ConsumerGroup
import com.easy.core.entity.Topic
import com.easy.server.dao.LocalPersistenceProvider
import com.easy.server.serverHandler.ConsumerInitMessageHandler
import com.easy.server.serverHandler.ConsumerToServerMessageHandler
import com.easy.server.serverHandler.InboundSpeedTestHandler
import com.easy.server.serverHandler.OutboundSpeedTestHandler
import com.easy.server.serverHandler.ProducerToServerMessageHandler
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelOption
import io.netty.channel.DefaultEventLoop
import io.netty.channel.WriteBufferWaterMark
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.serialization.ClassResolvers
import io.netty.handler.codec.serialization.ObjectDecoder
import io.netty.handler.codec.serialization.ObjectEncoder
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import java.util.concurrent.ConcurrentHashMap

@Service
class EasyServer(
    @Value("\${server.port}")
    val port: Int, val producerToServerMessageHandler: ProducerToServerMessageHandler,
    public val localPersistenceProvider: LocalPersistenceProvider,
    private val consumerInitMessageHandler: ConsumerInitMessageHandler,
    private val consumerToServerMessageHandler:ConsumerToServerMessageHandler
) {

    val topics = ConcurrentHashMap<String, Topic>();

    val consumerGroups = HashMap<String,ConsumerGroup>()



    fun run() {
        val serverBootstrap = ServerBootstrap()
        val bossGroup = NioEventLoopGroup()
        val workGroup = NioEventLoopGroup()
        val defaultEventLoop = DefaultEventLoop()
        try {
            serverBootstrap.group(bossGroup, workGroup)
                .channel(NioServerSocketChannel::class.java)
                .childHandler(object : ChannelInitializer<SocketChannel>() {
                    override fun initChannel(ch: SocketChannel?) {
                        ch!!.pipeline()
                           // .addLast(LoggingHandler(LogLevel.INFO))
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
                    }

                })

            val channelFuture = serverBootstrap.bind(port)
            channelFuture.sync()


            channelFuture.channel().closeFuture().sync()

        } catch (e: Exception) {

        }
    }
}