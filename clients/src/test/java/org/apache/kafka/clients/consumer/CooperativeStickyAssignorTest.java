/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.consumer;

import static org.apache.kafka.clients.consumer.CooperativeStickyAssignor.CooperativeUserData.VERSION_KEY_NAME;
import static org.apache.kafka.clients.consumer.CooperativeStickyAssignor.CooperativeUserData.decode;
import static org.apache.kafka.clients.consumer.StickyAssignor.GENERATION_KEY_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor.CooperativeUserData;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.junit.Test;

public class CooperativeStickyAssignorTest extends StickyAssignorTest {

    private static final int GENERATION = 5;

    @Test
    public void testEncodeDecodeUserData() {

        final CooperativeUserData userData = new CooperativeUserData(GENERATION);
        final ByteBuffer serializedUserData = userData.encode();

        assertTrue(userData.equals(decode(serializedUserData)));
    }

    @Test
    public void testForwardCompatability() {
        final Schema COOPERATIVE_STICKY_ASSIGNOR_USER_DATA_V1 = new Schema(
            new Field(VERSION_KEY_NAME, Type.INT16),
            new Field(GENERATION_KEY_NAME, Type.INT32),
            new Field("Some new field", Type.INT32));

        Struct struct = new Struct(COOPERATIVE_STICKY_ASSIGNOR_USER_DATA_V1);
        struct.set(VERSION_KEY_NAME, 2);
        struct.set(GENERATION_KEY_NAME, GENERATION);
        ByteBuffer buffer = ByteBuffer.allocate(COOPERATIVE_STICKY_ASSIGNOR_USER_DATA_V1.sizeOf(struct));
        COOPERATIVE_STICKY_ASSIGNOR_USER_DATA_V1.write(buffer, struct);

        // Make sure we can decode this as version 0
        CooperativeUserData userData = decode(buffer);
        assertEquals(userData.generation, GENERATION);
    }

}
