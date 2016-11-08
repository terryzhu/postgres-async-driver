package com.github.pgasync.dal;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.Arrays;
import java.util.List;

/**
 * Created by jie on 16/11/7.
 */
public class PGAheadPacketHander extends ByteToMessageDecoder {
    private int length = -1;
    private byte[] protocol = null;

    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list) throws Exception {
        if (length == -1) {
            if (byteBuf.readableBytes() < 4) {
                return;
            } else {
                length = byteBuf.readInt();
                protocol = new byte[length - 4];
            }
        }
        if (length != -1) {
            if (byteBuf.readableBytes() < length - 4) {
                return;
            }
            byteBuf.readBytes(protocol);

            // 判断是否是SSL尝试
            if (length == 8 && protocol.length == 4 && Arrays.equals(protocol, new byte[] { 0x04, (byte) 0xd2, 0x16, 0x2f })) {
                length = -1;
                channelHandlerContext.writeAndFlush(Unpooled.wrappedBuffer("N".getBytes()));
                return;
            }
            // 判断是否是StartupMessage
            byte[] first4Bytes = new byte[4];
            System.arraycopy(protocol, 0, first4Bytes, 0, 4);
            if (Arrays.equals(first4Bytes, new byte[] { 0x00, 0x03, 0x00, 0x00 })) {
                list.add(Unpooled.buffer().writeByte('I').writeInt(length).writeBytes(protocol));
                channelHandlerContext.pipeline().remove(this);
            }
        }
    }
}
