package com.easy.server

import com.easy.clientHandler.DataInboundCounter
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.ChannelInitializer
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.logging.LogLevel

fun main() {
    var serverBootstrap = ServerBootstrap()
    var bind =
        serverBootstrap.group(NioEventLoopGroup())

            .channel(NioServerSocketChannel::class.java).childHandler(object :
            ChannelInitializer<SocketChannel>() {
            override fun initChannel(ch: SocketChannel?) {
                ch!!.pipeline().addLast(DataInboundCounter())
            }
        }).bind(1000)
    bind.sync()
    bind.channel().closeFuture().sync()

}