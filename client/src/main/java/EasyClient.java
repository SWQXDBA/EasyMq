
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;

import java.util.Scanner;
import java.util.concurrent.ThreadFactory;


public class EasyClient {

    public EasyClient(int port, String host) {
        this.port = port;
        this.host = host;
    }

    private final int port;

    private final String host;

    public void run(){

        EventLoopGroup workerGroup = new NioEventLoopGroup(new ThreadFactory() {
            int i = 1;
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r,"workerGroup["+(i++)+"]");
            }
        });
        try {
            Bootstrap b = new Bootstrap();
            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.SO_KEEPALIVE, true);
            b.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addLast(new ObjectEncoder());
                    ch.pipeline().addLast(new ObjectDecoder(Integer.MAX_VALUE,ClassResolvers.weakCachingConcurrentResolver(this.getClass().getClassLoader())));
                }
            });

            ChannelFuture f = b.connect(host, port);
            f.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture channelFuture) throws Exception {
                        Scanner scanner = new Scanner(System.in);
                        Thread thread = new Thread(()->{
                            while(scanner.hasNext()){
                                final String str = scanner.nextLine();


                                if(str.equals("entity")){

                                }else{

                                }

                                channelFuture.channel().writeAndFlush(str);
                            }
                            });
                    thread.start();

                }
            });

            f.channel().closeFuture().sync();
        } catch (InterruptedException e){
            e.printStackTrace();
        }
        finally {
            workerGroup.shutdownGracefully();
        }
    }

    public void sendToTopic(Object msg,String Topic){

    }
}
