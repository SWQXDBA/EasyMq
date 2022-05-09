package com.easy;


import com.easy.core.message.ProducerToServerMessage;
import com.easy.core.message.ProducerToServerMessageUnit;
import com.easy.handler.ConnectActiveHandler;
import com.easy.handler.MessageRouter;
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

import java.util.Scanner;
import java.util.concurrent.atomic.AtomicLong;


public class EasyClient {



    private final int port;

    private final String host;
    private ChannelFuture channelFuture;

    ObjectMapper objectMapper = new ObjectMapper();

    AtomicLong idGenerator = new AtomicLong();

    MessageRouter messageRouter = new MessageRouter();
    ConnectActiveHandler connectActiveHandler ;

    /**
     *
     * @param port 要连接的服务器端口
     * @param host 服务器host如127.0.0.1
     * @param groupName 消费者组名 一条消息只会被消费者组中的某一个消费者消费到
     * @param consumerName 这个消费者的名字，不同消费者组的消费者可以重名，因为内部会把groupName和consumerName进行一个拼接
     */
    public EasyClient(int port, String host,String groupName,String consumerName) {
        this.port = port;
        this.host = host;
        connectActiveHandler = new ConnectActiveHandler(groupName,consumerName);
    }

    public void sendToTopic(Object message, String topicName) {
        final byte[] bytes;
        try {
            bytes = objectMapper.writeValueAsBytes(message);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return;
        }

        if (channelFuture == null) {
            System.out.println("channelFuture == null!!!");
            return;
        }
        ProducerToServerMessage producerToServerMessage = new ProducerToServerMessage();
        final ProducerToServerMessageUnit unit =
                new ProducerToServerMessageUnit(idGenerator.getAndIncrement(), bytes, topicName,message.getClass());
        producerToServerMessage.messages.add(unit);
        channelFuture.channel().writeAndFlush(producerToServerMessage);
    }

    public void addListener( EasyListener<?> listener) {
        System.out.println(listener);
        String topicName = listener.topicName;
        messageRouter.addListener(topicName, listener);
        connectActiveHandler.listenedTopics.add(topicName);
    }

    public void run() {
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.SO_KEEPALIVE, true);
            b.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addLast(new ObjectDecoder(Integer.MAX_VALUE,
                            ClassResolvers.weakCachingConcurrentResolver(this.getClass().getClassLoader())))
                            .addLast(new ObjectEncoder())
                            .addLast(connectActiveHandler)
                            .addLast(messageRouter);

                }
            });

            channelFuture = b.connect(host, port);
            channelFuture.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture channelFuture) throws Exception {
                    System.out.println("已连接 " + channelFuture.channel().remoteAddress());

                    Scanner scanner = new Scanner(System.in);
                    Thread thread = new Thread(() -> {
                        while (scanner.hasNext()) {
                            final String str = scanner.nextLine();
                            sendToTopic(str, "topic");
                        }
                    });
                    thread.start();

                }
            });

            channelFuture.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            workerGroup.shutdownGracefully();
        }
    }


}
