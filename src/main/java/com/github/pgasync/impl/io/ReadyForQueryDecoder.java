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

import com.github.pgasync.impl.message.ReadyForQuery;

import java.nio.ByteBuffer;

/**
 * See <a href="www.postgresql.org/docs/9.3/static/protocol-message-formats.html">PostgreSQL message formats</a>
 *
 * <pre>
 * ReadyForQuery (B)
 *  Byte1('Z')
 *      Identifies the message type. ReadyForQuery is sent whenever the backend is ready for a new query cycle.
 *  Int32(5)
 *      Length of message contents in bytes, including self.
 *  Byte1
 *      Current backend transaction status indicator. Possible values are 'I' if idle (not in a transaction block); 'T' if in a transaction block; or 'E' if in a failed transaction block (queries will be rejected until block is ended).
 * </pre>
 *
 * @author Antti Laisi
 */
public class ReadyForQueryDecoder implements Decoder<ReadyForQuery>,Encoder<ReadyForQuery> {

    @Override
    public ReadyForQuery read(ByteBuffer buffer) {
        buffer.get();
        return ReadyForQuery.INSTANCE;
    }

    @Override
    public byte getMessageId() {
        return 'Z';
    }

    @Override
    public Class<ReadyForQuery> getMessageType() {
        return ReadyForQuery.class;
    }

    @Override
    public void write(ReadyForQuery msg, ByteBuffer buffer) {
        buffer.put(getMessageId());
        buffer.putInt(0);
        buffer.put(ReadyForQuery.IDLE);
        buffer.putInt(1, buffer.position() - 1);
    }
}
