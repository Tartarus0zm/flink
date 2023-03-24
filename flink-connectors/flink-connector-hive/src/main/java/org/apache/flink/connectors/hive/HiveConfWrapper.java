/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connectors.hive;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import org.apache.hadoop.hive.conf.HiveConf;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/** Wrap {@link HiveConf} to a serializable class. */
public class HiveConfWrapper implements Serializable {

    private static final long serialVersionUID = 1L;

    private transient HiveConf hiveConf;

    public HiveConfWrapper(HiveConf hiveConf) {
        this.hiveConf = hiveConf;
    }

    public HiveConf conf() {
        return hiveConf;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();

        // we write the jobConf through a separate serializer to avoid cryptic exceptions when it
        // corrupts the serialization stream
        final DataOutputSerializer ser = new DataOutputSerializer(256);
        hiveConf.write(ser);
        out.writeInt(ser.length());
        out.write(ser.getSharedBuffer(), 0, ser.length());
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        final byte[] data = new byte[in.readInt()];
        in.readFully(data);
        final DataInputDeserializer deser = new DataInputDeserializer(data);
        this.hiveConf = new HiveConf();
        try {
            hiveConf.readFields(deser);
        } catch (IOException e) {
            throw new IOException(
                    "Could not deserialize hiveConf, the serialized and de-serialized don't match.",
                    e);
        }
    }
}
