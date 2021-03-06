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

package com.github.pgasync.impl.message;

import com.github.pgasync.impl.Oid;

/**
 * @author  Antti Laisi
 */
public class RowDescription implements Message {

    public static class ColumnDescription {

        final String name;
        public int tableOid;
        public short positionInTable;

        final Oid type;
        public int typeLength;
        public int typeModifier;
        public int formatType;


        public ColumnDescription(String name, Oid type) {
            this.name = name;
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public Oid getType() {
            return type;
        }
    }

    final ColumnDescription[] columns;

    public RowDescription(ColumnDescription[] columns) {
        this.columns = columns;
    }

    public ColumnDescription[] getColumns() {
        return columns;
    }
}
