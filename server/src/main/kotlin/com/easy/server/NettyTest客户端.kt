package com.easy.server

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBufAllocator
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelOption
import io.netty.channel.WriteBufferWaterMark
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.util.ReferenceCountUtil
import org.springframework.cglib.core.ReflectUtils

class NettyTest {
}

fun main() {
    var bootstrap = Bootstrap()
    var boss = NioEventLoopGroup()
    var channelFuture = bootstrap.group(boss)
        .option(ChannelOption.WRITE_BUFFER_WATER_MARK, WriteBufferWaterMark(1024*1024,1024*1024*4))

        .channel(NioSocketChannel::class.java)
        .handler(object :ChannelInitializer<SocketChannel>(){
            override fun initChannel(ch: SocketChannel?) {

            }

        })


        .connect("localhost",1000)
    channelFuture.sync()

    var byteArray = ByteArray(1024*1024*12)
    var channel = channelFuture.channel()
    while(true){
//        var buffer = Unpooled.buffer(1024*2)


        if(channel.isWritable){
            channel.writeAndFlush(Unpooled.wrappedBuffer(byteArray))
        }else{
            Thread.yield()
        }


    }

}
