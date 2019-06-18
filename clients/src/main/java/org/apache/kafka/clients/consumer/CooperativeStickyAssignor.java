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

import static org.apache.kafka.clients.consumer.CooperativeStickyAssignor.CooperativeUserData.decode;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;

/**
 * A cooperative version of the {@link StickyAssignor sticky PartitionAssignor}. This follows the same (Sticky)
 * assignment logic as {@code StickyAssignor} but allows for cooperative rebalancing and leverages the V1 Subscription's
 * {@code ownedPartitions} field rather than embedding this information in the {@link org.apache.kafka.clients.consumer.internals.ConsumerProtocol ConsumerProtocol's}
 * UserData.
 *
 * To turn on cooperative incremental rebalancing you must set all your consumers to use this {@code PartitionAssignor},
 * or implement a custom one that returns {@code RebalanceProtocol.COOPERATIVE} in {@link CooperativeStickyAssignor#supportedProtocols supportedProtocols()}.
 *
 * IMPORTANT: if upgrading from 2.3 or earlier, you must follow a specific upgrade path in order to safely turn on
 * cooperative rebalancing. See the upgrade-guide for details
 */
public class CooperativeStickyAssignor extends StickyAssignor {

    static final Schema COOPERATIVE_STICKY_ASSIGNOR_USER_DATA_V0 = new Schema(
            new Field(GENERATION_KEY_NAME, Type.INT32));

    static final class CooperativeUserData {
        final int generation;
        CooperativeUserData(int generation) {
            this.generation = generation;
        }

        ByteBuffer encode() {
            Struct struct = new Struct(COOPERATIVE_STICKY_ASSIGNOR_USER_DATA_V0);
            struct.set(GENERATION_KEY_NAME, generation);
            ByteBuffer buffer = ByteBuffer.allocate(COOPERATIVE_STICKY_ASSIGNOR_USER_DATA_V0.sizeOf(struct));
            COOPERATIVE_STICKY_ASSIGNOR_USER_DATA_V0.write(buffer, struct);

            return buffer;
        }

        static CooperativeUserData decode(ByteBuffer buffer) {
            Struct struct = COOPERATIVE_STICKY_ASSIGNOR_USER_DATA_V0.read(buffer);
            return new CooperativeUserData(struct.getInt(GENERATION_KEY_NAME));
        }
    }

    @Override
    public List<RebalanceProtocol> supportedProtocols() {
        return Arrays.asList(RebalanceProtocol.EAGER, RebalanceProtocol.COOPERATIVE);
    }

    @Override
    public String name() {
        return "cooperative-sticky";
    }

    @Override
    Subscription subscriptionInternal(Set<String> topics) {
        return new Subscription(
            new ArrayList<>(topics), new CooperativeUserData(generation()).encode(), memberAssignment());
    }

    @Override
    StickyUserData subscriptionToStickyUserData(Subscription subscription) {
        CooperativeUserData userData = decode(subscription.userData());
        return new StickyUserData(subscription.ownedPartitions(), Optional.of(userData.generation));
    }
}
