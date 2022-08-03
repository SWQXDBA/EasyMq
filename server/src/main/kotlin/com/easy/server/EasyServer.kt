package com.easy.server



import com.easy.clientHandler.DataInboundCounter
import com.easy.clientHandler.DataOutboundCounter
import com.easy.clientHandler.ReadableControl
import com.easy.core.entity.MessageId
import com.easy.server.core.entity.Consumer
import com.easy.server.core.entity.ConsumerGroup
import com.easy.server.core.entity.Producer
import com.easy.server.core.entity.Topic
import com.easy.server.dao.LocalPersistenceProvider
import com.easy.server.dao.PersistenceProvider
import com.easy.server.serverHandler.*
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.*
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.serialization.ClassResolvers
import io.netty.handler.codec.serialization.ObjectDecoder
import io.netty.handler.codec.serialization.ObjectEncoder
import io.netty.handler.logging.LogLevel
import io.netty.handler.logging.LoggingHandler
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import java.util.concurrent.ConcurrentHashMap

@Service
class EasyServer(
    @Value("\${server.port}")
    val port: Int, val producerToServerMessageHandler: ProducerToServerMessageHandler,
    public val persistenceProvider: PersistenceProvider,
    private val consumerInitMessageHandler: ConsumerInitMessageHandler,
    private val consumerToServerMessageHandler:ConsumerToServerMessageHandler,
    private val callBackMessageHandler: CallBackMessageHandler,

) {

    companion object{
        var INSTANSE :EasyServer? = null
    }

    private val dataInboundCounter: DataInboundCounter = DataInboundCounter()

    val topics = ConcurrentHashMap<String, Topic>();

    val consumerGroups = HashMap<String, ConsumerGroup>()


    init {
        Companion.INSTANSE = this
    }


    fun run() {

        val serverBootstrap = ServerBootstrap()
        val bossGroup = NioEventLoopGroup()
        val workGroup = NioEventLoopGroup()

        try {
            serverBootstrap.group(bossGroup, workGroup)
                .channel(NioServerSocketChannel::class.java)
                .childHandler(object : ChannelInitializer<SocketChannel>() {
                    override fun initChannel(ch: SocketChannel?) {
                        ch!!.pipeline()
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