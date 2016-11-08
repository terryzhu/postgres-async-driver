package com.github.pgasync.dal;

import com.github.pgasync.impl.netty.ByteBufMessageDecoder;
import com.github.pgasync.impl.netty.ByteBufMessageEncoder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

/**
 * Created by jie on 16/11/8.
 */
public class PGContext {
    public enum SIDE {SERVER, CLIENT}

    Channel server = null;
    Channel client = null;

    public void init(Channel client) throws Exception {
        this.client = client;
        new Bootstrap().group(PGDAL.serverGroup).channel(NioSocketChannel.class).handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                ChannelPipeline p = ch.pipeline();
                //                p.addLast(new LoggingHandler(LogLevel.INFO));
                p.addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 1, 4, -4, 0, true));
                p.addLast(new ByteBufMessageDecoder(PGContext.this,SIDE.SERVER));
                p.addLast(new ByteBufMessageEncoder(PGContext.this,SIDE.SERVER));
                server = ch;
            }
        }).connect("192.168.80.19", 5432).sync().addListener(f -> {
            if (!f.isSuccess()) {
                System.err.println("error when connecting");
            } else {
                System.out.println("connected!");
            }
        });
    }

    public void clientWriteAndFlush(Object msg) {
        client.writeAndFlush(msg);
    }

    public void serverWriteAndFlush(Object msg) {
        server.writeAndFlush(msg);
    }
}
