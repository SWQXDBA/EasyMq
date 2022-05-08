package com.easy.server


import com.easy.core.entity.Topic
import com.easy.server.Handler.ProducerToServerMessageHandler
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.ChannelInitializer
import io.netty.channel.DefaultEventLoop
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.LengthFieldBasedFrameDecoder
import io.netty.handler.codec.LengthFieldPrepender
import io.netty.handler.codec.serialization.ClassResolvers
import io.netty.handler.codec.serialization.ObjectDecoder
import io.netty.handler.codec.serialization.ObjectEncoder
import io.netty.handler.logging.LogLevel
import io.netty.handler.logging.LoggingHandler
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import java.lang.Exception
import java.util.concurrent.ConcurrentHashMap

@Service
class EasyServer (  @Value("\${server.port}")
                      val port:Int
                      ,val producerToServerMessageHandler: ProducerToServerMessageHandler
                      ) {

    val topics = ConcurrentHashMap<String,Topic>();

    fun run(){
        val serverBootstrap = ServerBootstrap()
        val bossGroup = NioEventLoopGroup()
        val workGroup = NioEventLoopGroup()
        val defaultEventLoop = DefaultEventLoop()

        try {
            serverBootstrap.group(bossGroup,workGroup)
                .channel(NioServerSocketChannel::class.java)
                .childHandler(object: ChannelInitializer<SocketChannel>() {
                    override fun initChannel(ch: SocketChannel?) {
                        ch!!.pipeline()
                            .addLast(LoggingHandler(LogLevel.INFO))
                            .addLast(ObjectDecoder(Int.MAX_VALUE,ClassResolvers.weakCachingConcurrentResolver(this::class.java.classLoader)))
                            .addLast(ObjectEncoder())
                            .addLast(producerToServerMessageHandler)

                    }

                })

            val channelFuture = serverBootstrap.bind(port)
            channelFuture.sync()

            channelFuture.channel().closeFuture().sync()

        }catch (e:Exception){

        }
    }
}