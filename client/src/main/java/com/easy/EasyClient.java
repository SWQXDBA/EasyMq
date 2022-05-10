package com.easy;


import com.easy.clientHandler.ConnectActiveHandler;
import com.easy.clientHandler.MessageRouter;
import com.easy.core.entity.MessageId;
import com.easy.core.message.ConsumerToServerMessage;
import com.easy.core.message.ProducerToServerMessage;
import com.easy.core.message.ProducerToServerMessageUnit;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;

import java.time.LocalDateTime;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;


public class EasyClient {


    private final int port;

    private final String host;

    private ChannelFuture channel;

    ObjectMapper objectMapper = new ObjectMapper();

    AtomicLong idGenerator = new AtomicLong();

    MessageRouter messageRouter = new MessageRouter();

    ConnectActiveHandler connectActiveHandler;

    String groupName;

    volatile ProducerToServerMessage currentMessageCache = new ProducerToServerMessage();
    volatile ConsumerToServerMessage consumerToServerMessage;

    /**
     * @param port         要连接的服务器端口
     * @param host         服务器host如127.0.0.1
     * @param groupName    消费者组名 一条消息只会被消费者组中的某一个消费者消费到
     * @param consumerName 这个消费者的名字，不同消费者组的消费者可以重名，因为内部会把groupName和consumerName进行一个拼接
     */
    public EasyClient(int port, String host, String groupName, String consumerName) {
        this.port = port;
        this.host = host;
        this.groupName = groupName;
        connectActiveHandler = new ConnectActiveHandler(groupName, consumerName);
        consumerToServerMessage = new ConsumerToServerMessage(groupName);
    }

    public void sendToTopic(Object message, String topicName) {
        final byte[] bytes;
        try {
            bytes = objectMapper.writeValueAsBytes(message);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return;
        }

        LocalDateTime now = LocalDateTime.now();
        final ProducerToServerMessageUnit unit =
                new ProducerToServerMessageUnit(now.getSecond() + "s " + idGenerator.getAndIncrement() + "", bytes, topicName, message.getClass());

        if (channel == null) {
            System.out.println("channelFuture == null!!!");
            currentMessageCache.messages.add(unit);
            return;
        }


        currentMessageCache.messages.add(unit);

        // channelFuture.channel().writeAndFlush(currentMessageCache);
    }

    public void addListener(EasyListener<?> listener) {
        System.out.println(listener);
        String topicName = listener.topicName;
        messageRouter.addListener(topicName, listener);
        connectActiveHandler.listenedTopics.add(topicName);
    }

    public void confirmationResponse(MessageId messageId) {
        consumerToServerMessage.confirmationResponse.add(messageId);
    }

    public void run() {
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        EventLoopGroup defaultEventLoop = new DefaultEventLoop(Executors.newSingleThreadExecutor());

        try {
            Bootstrap b = new Bootstrap();
            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.SO_KEEPALIVE, true);
            b.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline()
                            //    .addLast(new SpeedTestHandler())
                            .addLast(new ObjectDecoder(Integer.MAX_VALUE,
                                    ClassResolvers.weakCachingConcurrentResolver(this.getClass().getClassLoader())))
                            .addLast(new ObjectEncoder())
                            .addLast(connectActiveHandler)
                            .addLast(messageRouter);

                }
            });
            ChannelFuture channelFuture;
            channelFuture = b.connect(host, port);
            channelFuture.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture channelFuture) throws Exception {
                    System.out.println("已连接 " + channelFuture.channel().remoteAddress());
                    channel = channelFuture;
                }
            });
            defaultEventLoop.execute(() -> {
                while (true) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    //发送生产的消息
                    if (!this.currentMessageCache.messages.isEmpty()) {

                        final ProducerToServerMessage currentMessage = this.currentMessageCache;
                        this.currentMessageCache = new ProducerToServerMessage();
                        channelFuture.channel().writeAndFlush(currentMessage);
                    }
                    //回应收到的消息id
                    if (!this.consumerToServerMessage.confirmationResponse.isEmpty()) {
                        final ConsumerToServerMessage message = this.consumerToServerMessage;
                        this.consumerToServerMessage = new ConsumerToServerMessage(groupName);
                        channelFuture.channel().writeAndFlush(message);
                    }


                }
            });
            channelFuture.channel().closeFuture().sync();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            workerGroup.shutdownGracefully();
        }
    }


}
