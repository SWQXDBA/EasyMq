package com.easy.server


import io.netty.bootstrap.ServerBootstrap
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
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import java.lang.Exception
@Service
class EasyServer (  @Value("\${server.port}")
                      val port:Int) {


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
                            .addLast(ObjectDecoder(Int.MAX_VALUE,ClassResolvers.weakCachingConcurrentResolver(this::class.java.classLoader)))
                            .addLast(ObjectEncoder())

                    }

                })

            val channelFuture = serverBootstrap.bind(port)
            channelFuture.sync()

            channelFuture.channel().closeFuture().sync()

        }catch (e:Exception){

        }
    }
}