package com.easy;


import com.easy.clientHandler.CallBackMessageHandler;
import com.easy.clientHandler.ConnectActiveHandler;
import com.easy.clientHandler.ServerToConsumerMessageRouter;
import com.easy.core.entity.MessageId;
import com.easy.core.listener.EasyListener;
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
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;


public class EasyClient {


    private final int port;

    private final String host;

    private volatile ChannelFuture channel;

    ObjectMapper objectMapper = new ObjectMapper();

    AtomicLong idGenerator = new AtomicLong();

    ServerToConsumerMessageRouter serverToConsumerMessageRouter = new ServerToConsumerMessageRouter();

    CallBackMessageHandler callBackMessageHandler = new CallBackMessageHandler();

    ConnectActiveHandler connectActiveHandler;

    String groupName;

    volatile ProducerToServerMessage currentMessageCache = new ProducerToServerMessage();

    volatile ConsumerToServerMessage consumerToServerMessage;

    AtomicLong sentMessage = new AtomicLong();



    String clientName;

    /**
     * @param port       要连接的服务器端口
     * @param host       服务器host如127.0.0.1
     * @param groupName  消费者组名 一条消息只会被消费者组中的某一个消费者消费到
     * @param clientName 这个客户端的名字，不同消费者组的消费者可以重名，因为内部会把groupName和consumerName进行一个拼接
     */
    public EasyClient(int port, String host, String groupName, String clientName) {
        this.port = port;
        this.host = host;
        this.groupName = groupName;
        connectActiveHandler = new ConnectActiveHandler(groupName, clientName);
        consumerToServerMessage = new ConsumerToServerMessage(groupName);
        this.clientName = clientName;


    }

    public long getSentMessage() {
        return sentMessage.get();
    }

    public void sendToTopic(Object message, String topicName) {
        ProducerToServerMessageUnit unit;
        try {
            unit = buildMessageUnit(message, topicName);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return;
        }

        currentMessageCache.messages.add(unit);
        if (channel == null) {
            return;
        }
        if (channel.channel().isWritable()) {
            doSend();
        }
    }

    public <T>void sendAsync(Object message, String topicName,Consumer <T> callBack) {
        ProducerToServerMessageUnit unit;
        try {
            unit = buildMessageUnit(message, topicName);
            unit.callBack = true;
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return;
        }
        callBackMessageHandler.addCallBack(unit.getMessageId(),callBack);
        if (channel == null) {
            currentMessageCache.messages.add(unit);
        } else {
            //有回调的消息应该被急切地发送
            ProducerToServerMessage message1 = new ProducerToServerMessage();
            message1.messages.add(unit);
            channel.channel().writeAndFlush(message1);
        }

    }
    public <T> T sendSync(Object message, String topicName) {
        ProducerToServerMessageUnit unit;
        try {
            unit = buildMessageUnit(message, topicName);
            unit.callBack = true;
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }


        Thread thread =  Thread.currentThread();
        Object[] result =  new Object[1];
        Consumer <T> callBack = new Consumer<T>() {
            @Override
            public void accept(T t) {
                LockSupport.unpark(thread);
                result[0] = t;
            }
        };
        callBackMessageHandler.addCallBack(unit.getMessageId(),callBack);
        if (channel == null) {
            currentMessageCache.messages.add(unit);
        } else {
            //有回调的消息应该被急切地发送
            ProducerToServerMessage message1 = new ProducerToServerMessage();
            message1.messages.add(unit);
            channel.channel().writeAndFlush(message1);
        }
        LockSupport.park();
        return (T)result[0];

    }
    private ProducerToServerMessageUnit buildMessageUnit(Object message, String topicName) throws JsonProcessingException {
        final byte[] bytes;
        bytes = objectMapper.writeValueAsBytes(message);
        LocalDateTime now = LocalDateTime.now();

        //生成messageId
        var messageId = new MessageId();

        messageId.setTopicName(topicName);

        messageId.setProducerName(clientName);

        messageId.setUid(clientName + "=> " + now.getSecond() + "s " + idGenerator.getAndIncrement() + "");

        return
                new ProducerToServerMessageUnit(messageId, bytes, message.getClass());
    }


    public void addListener(EasyListener listener) {
        String topicName = listener.topicName;
        serverToConsumerMessageRouter.addListener(topicName, listener);
        connectActiveHandler.listenedTopics.add(topicName);
    }

    public void confirmationResponse(MessageId messageId) {
        consumerToServerMessage.confirmationResponse.add(messageId);
    }

    private synchronized void doSend() {
        //发送生产的消息
        if (!this.currentMessageCache.messages.isEmpty()) {
            final ProducerToServerMessage currentMessage = this.currentMessageCache;
            this.currentMessageCache = new ProducerToServerMessage();
            channel.channel().writeAndFlush(currentMessage);
            sentMessage.addAndGet(currentMessage.messages.size());
        }
        //回应收到的消息id

        if (!this.consumerToServerMessage.confirmationResponse.isEmpty()) {
            final ConsumerToServerMessage message = this.consumerToServerMessage;
            this.consumerToServerMessage = new ConsumerToServerMessage(groupName);
            channel.channel().writeAndFlush(message);
        }
    }

    private void connect() {
        EventLoopGroup workerGroup = new NioEventLoopGroup();


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
                            .addLast(serverToConsumerMessageRouter)
                            .addLast(callBackMessageHandler);


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

            channelFuture.channel().closeFuture().sync();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            workerGroup.shutdownGracefully();
        }

    }

    public void run() {

        EventLoopGroup defaultEventLoop = new DefaultEventLoop(Executors.newSingleThreadExecutor());
        defaultEventLoop.execute(() -> {
            while (true) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (channel == null || !channel.channel().isActive()) {
                    continue;
                }
                if (channel.channel().isWritable()) {
                    doSend();
                }
            }
        });
        while (true) {
            connect();
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


}
