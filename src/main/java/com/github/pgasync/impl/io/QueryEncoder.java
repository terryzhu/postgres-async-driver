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

package com.github.pgasync.impl.io;

import com.github.pgasync.impl.message.Query;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import static com.github.pgasync.impl.io.IO.putCString;

/**
 * See <a href="www.postgresql.org/docs/9.3/static/protocol-message-formats.html">PostgreSQL message formats</a>
 * <p>
 * <pre>
 * Query (F)
 *  Byte1('Q')
 *      Identifies the message as a simple query.
 *  Int32
 *      Length of message contents in bytes, including self.
 *  String
 *      The queryRows string itself.
 * </pre>
 *
 * @author Antti Laisi
 */
public class QueryEncoder implements Encoder<Query>, Decoder<Query> {

    @Override
    public Class<Query> getMessageType() {
        return Query.class;
    }

    @Override
    public void write(Query msg, ByteBuffer buffer) {
        buffer.put((byte) 'Q');
        buffer.putInt(0);
        putCString(buffer, msg.getQuery());
        buffer.putInt(1, buffer.position() - 1);
    }

    @Override
    public byte getMessageId() {
        return 'Q';
    }

    @Override
    public Query read(ByteBuffer buffer) {
        byte[] data = new byte[buffer.remaining()-1];
        buffer.get(data);
        buffer.get();
        String sql = new String(data, Charset.forName("utf8"));
        System.out.println("SQL: " + sql);
        return new Query(sql);
    }
}
