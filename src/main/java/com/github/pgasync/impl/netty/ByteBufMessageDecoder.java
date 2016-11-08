/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.pgasync.impl.netty;

import com.github.pgasync.dal.PGContext;
import com.github.pgasync.impl.io.*;
import com.github.pgasync.impl.message.Message;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Decodes incoming bytes to PostgreSQL V3 protocol message instances.
 *
 * @author Antti Laisi
 */
public class ByteBufMessageDecoder extends ByteToMessageDecoder {

    static final Map<Byte, Decoder<?>> DECODERS = new HashMap<>();

    static {
        // @formatter:off
        for (Decoder<?> decoder : new Decoder<?>[] {
                new StartupMessageEncoder(),
//                new ErrorResponseDecoder(),
                new AuthenticationDecoder(),
                new PasswordMessageEncoder(),
                new ReadyForQueryDecoder(),
                new QueryEncoder(),
                new RowDescriptionDecoder(),
//                new CommandCompleteDecoder(),
//                new DataRowDecoder(),
                new NotificationResponseDecoder()}) {
            DECODERS.put(decoder.getMessageId(), decoder);
        }
        // @formatter:on
    }

    private final PGContext pgctx;
    private PGContext.SIDE side = null;

    public ByteBufMessageDecoder(PGContext pgContext, PGContext.SIDE side) {
        this.pgctx = pgContext;
        this.side = side;
    }

    public ByteBufMessageDecoder() {
        this.pgctx = null;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (in.readableBytes() == 0) {
            return;
        }

        byte id = in.readByte();
        int length = in.readInt();

        Decoder<?> decoder = DECODERS.get(id);
        if (decoder == null) {
            System.out.println((char) id + " has no decoder: ");
        }
        try {
            if (decoder != null) {
                ByteBuffer buffer = in.nioBuffer();
                //                out.add(decoder.read(buffer));
                Message msg = decoder.read(buffer);
                in.skipBytes(buffer.position());
                if (side == PGContext.SIDE.CLIENT) {
                    pgctx.serverWriteAndFlush(msg);
                } else {
                    pgctx.clientWriteAndFlush(msg);
                }
            } else {
                ByteBuf buffer = Unpooled.buffer().writeByte(id).writeInt(length).writeBytes(in.nioBuffer());
                in.skipBytes(length - 4);
                if (side == PGContext.SIDE.CLIENT) {
                    pgctx.serverWriteAndFlush(buffer);
                } else {
                    pgctx.clientWriteAndFlush(buffer);
                }
            }
        } catch (Throwable t) {
            // broad catch as otherwise the exception is silently dropped
            ctx.fireExceptionCaught(t);
        }
    }
}
