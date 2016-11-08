package com.github.pgasync.dal;

import com.github.pgasync.ConnectionPoolBuilder;
import com.github.pgasync.Db;
import com.github.pgasync.impl.netty.ByteBufMessageDecoder;
import com.github.pgasync.impl.netty.ByteBufMessageEncoder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import java.io.IOException;
import java.net.Socket;

/**
 * Created by jie on 16/11/7.
 */
public class PGDAL {
    public static EventLoopGroup serverGroup = new NioEventLoopGroup(2);

    public static void main(String[] args) throws Exception {
        //        Db db = new ConnectionPoolBuilder().hostname("192.168.80.19").port(5432).database("pgtestdb").username("root").password("root").poolSize(20).build();
        //        db.query("select * from eleme_order", results -> results.forEach(row -> {
        //            System.out.println(row.getBigInteger(1));
        //        }), e -> {
        //        });
        //        db.close();

        startServer();
    }

    private static void startServer() throws Exception {
        ServerBootstrap bootstrap = new ServerBootstrap().group(serverGroup).channel(NioServerSocketChannel.class);
        bootstrap.option(ChannelOption.SO_BACKLOG, 100).handler(new LoggingHandler(LogLevel.INFO)).childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                ChannelPipeline p = ch.pipeline();
                PGContext pgctx = new PGContext();
                pgctx.init(ch);
                //p.addLast(new LoggingHandler(LogLevel.INFO));
                p.addLast(new PGAheadPacketHander());
                p.addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 1, 4, -4, 0, true));
                p.addLast(new ByteBufMessageDecoder(pgctx, PGContext.SIDE.CLIENT));
                p.addLast(new ByteBufMessageEncoder(pgctx, PGContext.SIDE.CLIENT));
            }
        });
        bootstrap.bind(8899).sync();

        //        System.out.println("hello");
        //        Thread.sleep(5000);
        //
        //        Socket s = new Socket("127.0.0.1", 8899);
        //        s.getOutputStream().write("a".getBytes());
        //        s.getOutputStream().flush();
        //        Thread.sleep(3000);
        //        s.getOutputStream().write("b".getBytes());
        //        s.getOutputStream().flush();
        //        Thread.sleep(10000000);
    }
}
