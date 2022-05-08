package com.easy;


import com.easy.core.entity.Person;
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
import org.springframework.boot.json.GsonJsonParser;
import org.springframework.boot.json.JacksonJsonParser;

import java.util.Scanner;
import java.util.concurrent.atomic.AtomicLong;


public class EasyClient {

    ObjectMapper objectMapper = new ObjectMapper();
    public EasyClient(int port, String host) {
        this.port = port;
        this.host = host;
    }

    private final int port;

    private final String host;

    AtomicLong idGenerator = new AtomicLong();
    private ChannelFuture channelFuture;

    public void sendToTopic(Object message, String topicName) {
        final byte[] bytes;
        try {
            bytes  = objectMapper.writeValueAsBytes(message);
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
                new ProducerToServerMessageUnit(idGenerator.getAndIncrement(), bytes, topicName);
        producerToServerMessage.messages.add(unit);
        channelFuture.channel().writeAndFlush(producerToServerMessage);
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
                            ClassResolvers.weakCachingConcurrentResolver(this.getClass().getClassLoader())));
                    ch.pipeline().addLast(new ObjectEncoder());

                }
            });

            channelFuture = b.connect(host, port);
            channelFuture.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture channelFuture) throws Exception {
                    System.out.println("已连接 "+channelFuture.channel().remoteAddress().getClass());
                    Scanner scanner = new Scanner(System.in);
                    Thread thread = new Thread(() -> {
                        while (scanner.hasNext()) {
                            final String str = scanner.nextLine();
                            sendToTopic(new Person(str), "");
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
