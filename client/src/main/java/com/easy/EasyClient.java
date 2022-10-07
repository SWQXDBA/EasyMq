package com.easy;


import com.easy.clientHandler.*;
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
import io.netty.handler.timeout.IdleStateHandler;
import kotlin.Pair;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;


public class EasyClient {

    class Node {
        int port;
        String host;

        public Node(int port, String host) {
            this.port = port;
            this.host = host;
        }
    }


    private List<Node> serverNodes = new ArrayList<>();

    AtomicLong nodeSelector = new AtomicLong();

    private volatile ChannelFuture channel;

    ObjectMapper objectMapper = new ObjectMapper();

    AtomicLong idGenerator = new AtomicLong();

    ServerToConsumerMessageRouter serverToConsumerMessageRouter = new ServerToConsumerMessageRouter();

    CallBackMessageHandler callBackMessageHandler = new CallBackMessageHandler();

    ConnectActiveHandler connectActiveHandler;

    ServerToProducerMessageHandler serverToProducerMessageHandler = new ServerToProducerMessageHandler(this);

    DataInboundCounter dataInboundCounter = new DataInboundCounter();
    DataOutboundCounter dataOutboundCounter = new DataOutboundCounter();

    String groupName;

    volatile ProducerToServerMessage currentMessageCache = new ProducerToServerMessage();

    volatile ConsumerToServerMessage consumerToServerMessage;

    AtomicLong sentMessage = new AtomicLong();

    EventLoopGroup defaultEventLoop = new DefaultEventLoop(Executors.newFixedThreadPool(19));


    ExecutorService asyncSendingExecutor = Executors.newCachedThreadPool();

    Set<MessageId> nonConfirmedMessages = ConcurrentHashMap.newKeySet(1024);

    String clientName;

    /**
     * @param port       要连接的服务器端口
     * @param host       服务器host如127.0.0.1
     * @param groupName  消费者组名 一条消息只会被消费者组中的某一个消费者消费到
     * @param clientName 这个客户端的名字，不同消费者组的消费者可以重名，因为内部会把groupName和consumerName进行一个拼接
     */
    public EasyClient(int port, String host, String groupName, String clientName) {
        serverNodes.add(new Node(port,host));
        if(groupName!=null){
            this.groupName = groupName;
            connectActiveHandler = new ConnectActiveHandler(groupName, clientName);
            consumerToServerMessage = new ConsumerToServerMessage(groupName);
        }
        this.clientName = clientName;
    }

    public void addNode(int port, String host){
        serverNodes.add(new Node(port,host));
    }

    public long getSentMessage() {
        return sentMessage.get();
    }

    void confirmedProducerMessage(MessageId messageId) {
        nonConfirmedMessages.remove(messageId);
    }

    /**
     * 把消息进行单向发布
     *
     * 这里的缓存实现有点问题，如果说因为无法写入而在currentMessageCache中存了太多数据，序列化会耗费可能几分钟的时间，
     * 所以说不能一次把缓存中的所有数据都发送出去。
     * @param message
     * @param topicName
     */
    public void sendToTopic(Object message, String topicName) {
        ProducerToServerMessageUnit unit;
        try {
            unit = buildMessageUnit(message, topicName);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return;
        }

        nonConfirmedMessages.add(unit.messageId);


        currentMessageCache.messages.add(unit);
        if (channel == null) {
            return;
        }
        if (channel.channel().isWritable()&&this.currentMessageCache.messages.size()>=500
        ||this.currentMessageCache.messages.size()>=1000
        ) {
            doSend();
        }
    }

    /**
     * 发送一条消息，并且异步处理回调结果，必须保证不会有多个消费者组返回回调信息
     *
     * @param message
     * @param topicName
     * @param callBack
     * @param <T>
     */
    public <T> void sendAsync(Object message, String topicName, Consumer<T> callBack) {
        ProducerToServerMessageUnit unit;
        try {
            unit = buildMessageUnit(message, topicName);
            unit.callBack = true;
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return;
        }
        callBackMessageHandler.addCallBack(unit.getMessageId(), callBack);
        if (channel == null) {
            currentMessageCache.messages.add(unit);
        } else {
            //有回调的消息应该被急切地发送
            ProducerToServerMessage message1 = new ProducerToServerMessage();
            message1.messages.add(unit);
            nonConfirmedMessages.add(unit.messageId);
            channel.channel().writeAndFlush(message1);
        }

    }

    public <T> Future<T> sendAsync(Object message, String topicName) {
        return asyncSendingExecutor.submit(() -> {
            System.out.println("666");
            Object[] result = new Object[1];
            Thread thread = Thread.currentThread();
            sendAsync(message, topicName, t -> {
                result[0] = t;
                LockSupport.unpark(thread);
            });
            LockSupport.park();
            return (T) result[0];
        });
    }

    /**
     * 发送一条消息，并且阻塞等待处理结果，必须保证不会有多个消费者组返回回调信息
     *
     * @param message
     * @param topicName
     * @param <T>
     * @return
     */
    public <T> T sendSync(Object message, String topicName) {
        Thread thread = Thread.currentThread();
        Object[] result = new Object[1];
        sendAsync(message, topicName, t -> {
            result[0] = t;
            LockSupport.unpark(thread);
        });
        LockSupport.park();
        return (T) result[0];

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
        if(channel.channel().isWritable()){
            doSend();
        }

    }

    private synchronized void doSend() {

//        System.out.println("send "+ channel.channel().isWritable() +currentMessageCache.messages.size());
        //发送生产的消息
        if (!this.currentMessageCache.messages.isEmpty()) {
            final ProducerToServerMessage currentMessage = this.currentMessageCache;

            this.currentMessageCache = new ProducerToServerMessage();
            channel.channel().writeAndFlush(currentMessage);
            sentMessage.addAndGet(currentMessage.messages.size());

        }
        if(this.groupName!=null){
            //回应收到的消息id
            if (!this.consumerToServerMessage.confirmationResponse.isEmpty()) {
                final ConsumerToServerMessage message = this.consumerToServerMessage;
                this.consumerToServerMessage = new ConsumerToServerMessage(groupName);
                channel.channel().writeAndFlush(message);
            }
        }


    }

    private Node nextNode(){
        final long index = nodeSelector.getAndIncrement() % serverNodes.size();
        return serverNodes.get((int)index);
    }

    private void connect() {
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        DefaultEventLoopGroup eventExecutors = new DefaultEventLoopGroup(10);

        try {
            Bootstrap b = new Bootstrap();
            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.SO_KEEPALIVE, true);
            b.option(ChannelOption.WRITE_BUFFER_WATER_MARK,new WriteBufferWaterMark(1024*1024,1024*1024*4));
            b.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline()
                            //客户端3秒内没有发送消息 则发送心跳包
                            .addLast(new IdleStateHandler(0,3,0, TimeUnit.SECONDS))
                            //心跳包检测 这里需要放在ObjectDecoder之后因为心跳是一个String " " 需要编码后才能发送出去
                            .addLast(new IdleHandler())
                            .addLast(dataOutboundCounter)
                            //    .addLast(new SpeedTestHandler())
                            .addLast(eventExecutors,new ObjectDecoder(Integer.MAX_VALUE,
                                    ClassResolvers.weakCachingConcurrentResolver(this.getClass().getClassLoader())))
                            .addLast(eventExecutors,new ObjectEncoder())


                            .addLast(connectActiveHandler)
                            .addLast(serverToConsumerMessageRouter)
                            .addLast(callBackMessageHandler)
                            .addLast(serverToProducerMessageHandler);


                }
            });
            ChannelFuture channelFuture;
            final Node node = nextNode();
            channelFuture = b.connect(node.host, node.port);
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


        defaultEventLoop.execute(() -> {
            while (true) {
                try {
                    Thread.sleep(200);
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
