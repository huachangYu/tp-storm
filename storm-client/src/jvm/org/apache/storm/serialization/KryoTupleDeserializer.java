/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.serialization;

import com.esotericsoftware.kryo.io.Input;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.tuple.MessageId;
import org.apache.storm.tuple.TupleImpl;

public class KryoTupleDeserializer implements ITupleDeserializer {
    private GeneralTopologyContext context;
    private KryoValuesDeserializer kryo;
    private SerializationFactory.IdDictionary ids;
    private Input kryoInput;

    public KryoTupleDeserializer(final Map<String, Object> conf, final GeneralTopologyContext context) {
        kryo = new KryoValuesDeserializer(conf);
        this.context = context;
        ids = new SerializationFactory.IdDictionary(context.getRawTopology());
        kryoInput = new Input(1);
    }

    @Override
    public TupleImpl deserialize(byte[] ser) {
        try {
            kryoInput.setBuffer(ser);
            int taskId = kryoInput.readInt(true);
            int streamId = kryoInput.readInt(true);
            long rootId = kryoInput.readLong(true);
            String componentName = context.getComponentId(taskId);
            String streamName = ids.getStreamName(componentName, streamId);
            MessageId id = MessageId.deserialize(kryoInput);
            List<Object> values = kryo.deserializeFrom(kryoInput);
            return new TupleImpl(context, values, componentName, taskId, streamName, id, rootId);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
