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
package org.apache.kafka.clients.consumer.internals;

import java.util.Optional;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * ConsumerProtocol contains the schemas for consumer subscriptions and assignments for use with
 * Kafka's generalized group management protocol. Below is the version 1 format:
 *
 * <pre>
 * Subscription => Version Topics
 *   Version    => Int16
 *   Topics     => [String]
 *   UserData   => Bytes
 *   OwnedPartitions    => [Topic Partitions]
 *     Topic            => String
 *     Partitions       => [int32]
 *
 * Assignment => Version TopicPartitions
 *   Version            => int16
 *   AssignedPartitions => [Topic Partitions]
 *     Topic            => String
 *     Partitions       => [int32]
 *   UserData           => Bytes
 *   ErrorCode          => [int16]
 * </pre>
 *
 * Version 0 format:
 *
 * <pre>
 * Subscription => Version Topics
 *   Version    => Int16
 *   Topics     => [String]
 *   UserData   => Bytes
 *
 * Assignment => Version TopicPartitions
 *   Version            => int16
 *   AssignedPartitions => [Topic Partitions]
 *     Topic            => String
 *     Partitions       => [int32]
 *   UserData           => Bytes
 * </pre>
 *
 *
 * The current implementation assumes that future versions will not break compatibility. When
 * it encounters a newer version, it parses it using the current format. This basically means
 * that new versions cannot remove or reorder any of the existing fields.
 */
public class ConsumerProtocol {

    public static final String PROTOCOL_TYPE = "consumer";

    public static final String VERSION_KEY_NAME = "version";
    public static final String TOPICS_KEY_NAME = "topics";
    public static final String TOPIC_KEY_NAME = "topic";
    public static final String PARTITIONS_KEY_NAME = "partitions";
    public static final String OWNED_PARTITIONS_KEY_NAME = "owned_partitions";
    public static final String TOPIC_PARTITIONS_KEY_NAME = "topic_partitions";
    public static final String CONSUMER_DATA_KEY_NAME = "consumer_data";
    public static final String USER_DATA_KEY_NAME = "user_data";

    public static final Field.Int16 ERROR_CODE = new Field.Int16("error_code", "Assignment error code");

    public static final short CONSUMER_PROTOCOL_V0 = 0;
    public static final short CONSUMER_PROTOCOL_V1 = 1;

    public static final Schema CONSUMER_PROTOCOL_HEADER_SCHEMA = new Schema(
            new Field(VERSION_KEY_NAME, Type.INT16));
    private static final Struct CONSUMER_PROTOCOL_HEADER_V0 = new Struct(CONSUMER_PROTOCOL_HEADER_SCHEMA)
            .set(VERSION_KEY_NAME, CONSUMER_PROTOCOL_V0);
    private static final Struct CONSUMER_PROTOCOL_HEADER_V1 = new Struct(CONSUMER_PROTOCOL_HEADER_SCHEMA)
            .set(VERSION_KEY_NAME, CONSUMER_PROTOCOL_V1);

    public static final Schema TOPIC_ASSIGNMENT_V0 = new Schema(
        new Field(TOPIC_KEY_NAME, Type.STRING),
        new Field(PARTITIONS_KEY_NAME, new ArrayOf(Type.INT32)));

    public static final Schema CONSUMER_SUBSCRIPTION_V0 = new Schema(
            new Field(TOPICS_KEY_NAME, new ArrayOf(Type.STRING));

    public static final Schema CONSUMER_SUBSCRIPTION_V1 = new Schema(
        new Field(TOPICS_KEY_NAME, new ArrayOf(Type.STRING)),
        new Field(OWNED_PARTITIONS_KEY_NAME, new ArrayOf(TOPIC_ASSIGNMENT_V0)));

    public static final Schema ASSIGNMENT_V0 = new Schema(
            new Field(TOPIC_PARTITIONS_KEY_NAME, new ArrayOf(TOPIC_ASSIGNMENT_V0)),
            new Field(USER_DATA_KEY_NAME, Type.NULLABLE_BYTES));

    public static final Schema ASSIGNMENT_V1 = new Schema(
        new Field(TOPIC_PARTITIONS_KEY_NAME, new ArrayOf(TOPIC_ASSIGNMENT_V0)),
        new Field(USER_DATA_KEY_NAME, Type.NULLABLE_BYTES),
        ERROR_CODE);

    public enum AssignmentError {
        NONE(0),
        NEED_REJOIN(1);

        private final short code;

        AssignmentError(final int code) {
            this.code = (short) code;
        }

        public short code() {
            return code;
        }

        public static AssignmentError fromCode(final short code) {
            switch (code) {
                case 0:
                    return NONE;
                case 1:
                    return NEED_REJOIN;
                default:
                    throw new IllegalArgumentException("Unknown error code: " + code);
            }
        }
    }



    public static ByteBuffer serializeAssignmentV0(Assignment assignment) {
        Struct struct = new Struct(ASSIGNMENT_V0);
        struct.set(USER_DATA_KEY_NAME, assignment.userData());
        List<Struct> topicAssignments = new ArrayList<>();
        Map<String, List<Integer>> partitionsByTopic = CollectionUtils.groupPartitionsByTopic(assignment.partitions());
        for (Map.Entry<String, List<Integer>> topicEntry : partitionsByTopic.entrySet()) {
            Struct topicAssignment = new Struct(TOPIC_ASSIGNMENT_V0);
            topicAssignment.set(TOPIC_KEY_NAME, topicEntry.getKey());
            topicAssignment.set(PARTITIONS_KEY_NAME, topicEntry.getValue().toArray());
            topicAssignments.add(topicAssignment);
        }
        struct.set(TOPIC_PARTITIONS_KEY_NAME, topicAssignments.toArray());

        ByteBuffer buffer = ByteBuffer.allocate(CONSUMER_PROTOCOL_HEADER_V0.sizeOf() + ASSIGNMENT_V0.sizeOf(struct));
        CONSUMER_PROTOCOL_HEADER_V0.writeTo(buffer);
        ASSIGNMENT_V0.write(buffer, struct);
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer serializeAssignmentV1(Assignment assignment) {
        Struct struct = new Struct(ASSIGNMENT_V1);
        struct.set(USER_DATA_KEY_NAME, assignment.userData());
        List<Struct> topicAssignments = new ArrayList<>();
        Map<String, List<Integer>> partitionsByTopic = CollectionUtils.groupPartitionsByTopic(assignment.partitions());
        for (Map.Entry<String, List<Integer>> topicEntry : partitionsByTopic.entrySet()) {
            Struct topicAssignment = new Struct(TOPIC_ASSIGNMENT_V0);
            topicAssignment.set(TOPIC_KEY_NAME, topicEntry.getKey());
            topicAssignment.set(PARTITIONS_KEY_NAME, topicEntry.getValue().toArray());
            topicAssignments.add(topicAssignment);
        }
        struct.set(TOPIC_PARTITIONS_KEY_NAME, topicAssignments.toArray());
        struct.set(ERROR_CODE, assignment.error().code);

        ByteBuffer buffer = ByteBuffer.allocate(CONSUMER_PROTOCOL_HEADER_V1.sizeOf() + ASSIGNMENT_V1.sizeOf(struct));
        CONSUMER_PROTOCOL_HEADER_V1.writeTo(buffer);
        ASSIGNMENT_V1.write(buffer, struct);
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer serializeAssignment(Assignment assignment) {
        switch (assignment.version()) {
            case CONSUMER_PROTOCOL_V0:
                return serializeAssignmentV0(assignment);

            case CONSUMER_PROTOCOL_V1:
                return serializeAssignmentV1(assignment);

            default:
                // for any versions higher than known, try to serialize it as V1
                return serializeAssignmentV1(assignment);
        }
    }

    public static Assignment deserializeAssignmentV0(ByteBuffer buffer) {
        Struct struct = ASSIGNMENT_V0.read(buffer);
        ByteBuffer userData = struct.getBytes(USER_DATA_KEY_NAME);
        List<TopicPartition> partitions = new ArrayList<>();
        for (Object structObj : struct.getArray(TOPIC_PARTITIONS_KEY_NAME)) {
            Struct assignment = (Struct) structObj;
            String topic = assignment.getString(TOPIC_KEY_NAME);
            for (Object partitionObj : assignment.getArray(PARTITIONS_KEY_NAME)) {
                partitions.add(new TopicPartition(topic, (Integer) partitionObj));
            }
        }
        return new Assignment(CONSUMER_PROTOCOL_V0, partitions, userData);
    }

    public static Assignment deserializeAssignmentV1(ByteBuffer buffer) {
        Struct struct = ASSIGNMENT_V1.read(buffer);
        ByteBuffer userData = struct.getBytes(USER_DATA_KEY_NAME);
        List<TopicPartition> partitions = new ArrayList<>();
        for (Object structObj : struct.getArray(TOPIC_PARTITIONS_KEY_NAME)) {
            Struct assignment = (Struct) structObj;
            String topic = assignment.getString(TOPIC_KEY_NAME);
            for (Object partitionObj : assignment.getArray(PARTITIONS_KEY_NAME)) {
                partitions.add(new TopicPartition(topic, (Integer) partitionObj));
            }
        }

        AssignmentError error = AssignmentError.fromCode(struct.get(ERROR_CODE));

        return new Assignment(CONSUMER_PROTOCOL_V1, partitions, userData, error);
    }

    public static Assignment deserializeAssignment(ByteBuffer buffer) {
        Struct header = CONSUMER_PROTOCOL_HEADER_SCHEMA.read(buffer);
        Short version = header.getShort(VERSION_KEY_NAME);

        if (version < CONSUMER_PROTOCOL_V0)
            throw new SchemaException("Unsupported assignment version: " + version);

        switch (version) {
            case CONSUMER_PROTOCOL_V0:
                return deserializeAssignmentV0(buffer);

            case CONSUMER_PROTOCOL_V1:
                return deserializeAssignmentV1(buffer);

            default:
                // assume all higher versions can be parsed as V1
                return deserializeAssignmentV1(buffer);
        }
    }

    public static ByteBuffer serializeSubscription(Subscription subscription) {
        ByteBuffer buffer = ByteBuffer.allocate(subscription.consumerData.limit() + subscription.userData.limit());
        buffer.put(subscription.consumerData);
        buffer.put(subscription.userData);
        return buffer;
    }

    static class Subscription {
        private final ByteBuffer consumerData;
        private final ByteBuffer userData;

        public Subscription(ByteBuffer consumerData, ByteBuffer userData) {
            this.consumerData = consumerData;

            this.userData = userData;
        }

        public ByteBuffer consumerSubscriptionData() {
            return consumerData;
        }

        public ByteBuffer userData() {
            return userData;
        }
    }

    static class ConsumerSubscriptionData {

        private final Short version;
        private final List<String> topics;
        private final List<TopicPartition> ownedPartitions;
        private Optional<String> groupInstanceId;

        ConsumerSubscriptionData(Short version,
            List<String> topics,
            List<TopicPartition> ownedPartitions) {
            this.version = version;
            this.topics = topics;
            this.ownedPartitions = ownedPartitions;
            this.groupInstanceId = Optional.empty();

            if (version < CONSUMER_PROTOCOL_V0)
                throw new SchemaException("Unsupported subscription version: " + version);

            if (version < CONSUMER_PROTOCOL_V1 && !ownedPartitions.isEmpty())
                throw new IllegalArgumentException("Subscription version smaller than 1 should not have owned partitions");
        }

        public ConsumerSubscriptionData(List<String> topics, List<TopicPartition> ownedPartitions) {
            this(CONSUMER_PROTOCOL_V1, topics, ownedPartitions);
        }

        Short version() {
            return version;
        }

        public List<String> topics() {
            return topics;
        }

        public List<TopicPartition> ownedPartitions() {
            return ownedPartitions;
        }

        public void setGroupInstanceId(Optional<String> groupInstanceId) {
            this.groupInstanceId = groupInstanceId;
        }

        public Optional<String> groupInstanceId() {
            return groupInstanceId;
        }

        public static ByteBuffer serializeConsumerSubscriptionData(ConsumerSubscriptionData consumerdata) {
            switch (consumerdata.version()) {
                case CONSUMER_PROTOCOL_V0:
                    return serializeSubscriptionV0(consumerdata);

                case CONSUMER_PROTOCOL_V1:
                    return serializeSubscriptionV1(consumerdata);

                default:
                    // for any versions higher than known, try to serialize it as V1
                    return serializeSubscriptionV1(consumerdata);
            }
        }

        static ByteBuffer serializeSubscriptionV0(ConsumerSubscriptionData consumerdata) {
            Struct struct = new Struct(CONSUMER_SUBSCRIPTION_V0);
            struct.set(TOPICS_KEY_NAME, consumerdata.topics().toArray());

            ByteBuffer buffer = ByteBuffer.allocate(CONSUMER_PROTOCOL_HEADER_V0.sizeOf() + CONSUMER_SUBSCRIPTION_V0.sizeOf(struct));
            CONSUMER_PROTOCOL_HEADER_V0.writeTo(buffer);
            CONSUMER_SUBSCRIPTION_V0.write(buffer, struct);
            buffer.flip();
            return buffer;
        }

        public static ByteBuffer serializeSubscriptionV1(ConsumerSubscriptionData consumerdata) {
            Struct struct = new Struct(CONSUMER_SUBSCRIPTION_V1);
            struct.set(TOPICS_KEY_NAME, consumerdata.topics().toArray());
            List<Struct> topicAssignments = new ArrayList<>();
            Map<String, List<Integer>> partitionsByTopic = CollectionUtils.groupPartitionsByTopic(consumerdata.ownedPartitions());
            for (Map.Entry<String, List<Integer>> topicEntry : partitionsByTopic.entrySet()) {
                Struct topicAssignment = new Struct(TOPIC_ASSIGNMENT_V0);
                topicAssignment.set(TOPIC_KEY_NAME, topicEntry.getKey());
                topicAssignment.set(PARTITIONS_KEY_NAME, topicEntry.getValue().toArray());
                topicAssignments.add(topicAssignment);
            }
            struct.set(OWNED_PARTITIONS_KEY_NAME, topicAssignments.toArray());

            ByteBuffer buffer = ByteBuffer.allocate(CONSUMER_PROTOCOL_HEADER_V1.sizeOf() + CONSUMER_SUBSCRIPTION_V1.sizeOf(struct));
            CONSUMER_PROTOCOL_HEADER_V1.writeTo(buffer);
            CONSUMER_SUBSCRIPTION_V1.write(buffer, struct);
            buffer.flip();
            return buffer;
        }

        @Override
        public String toString() {
            return "Subscription(" +
                "version=" + version +
                ", topics=" + topics +
                ", ownedPartitions=" + ownedPartitions +
                ", group.instance.id=" + groupInstanceId + ")";
        }
    }

    static class Assignment {
        private final Short version;
        private List<TopicPartition> partitions;
        private final ByteBuffer userData;
        private ConsumerProtocol.AssignmentError error;

        Assignment(Short version, List<TopicPartition> partitions, ByteBuffer userData, ConsumerProtocol.AssignmentError error) {
            this.version = version;
            this.partitions = partitions;
            this.userData = userData;
            this.error = error;

            if (version < CONSUMER_PROTOCOL_V0)
                throw new SchemaException("Unsupported subscription version: " + version);

            if (version < CONSUMER_PROTOCOL_V1 && error != ConsumerProtocol.AssignmentError.NONE)
                throw new IllegalArgumentException("Assignment version smaller than 1 should not have error code.");
        }

        Assignment(Short version, List<TopicPartition> partitions, ByteBuffer userData) {
            this(version, partitions, userData, ConsumerProtocol.AssignmentError.NONE);
        }

        public Assignment(List<TopicPartition> partitions, ByteBuffer userData) {
            this(CONSUMER_PROTOCOL_V1, partitions, userData);
        }

        public Assignment(List<TopicPartition> partitions) {
            this(partitions, ByteBuffer.wrap(new byte[0]));
        }

        Short version() {
            return version;
        }

        public List<TopicPartition> partitions() {
            return partitions;
        }

        public ConsumerProtocol.AssignmentError error() {
            return error;
        }

        public void updatePartitions(List<TopicPartition> partitions) {
            this.partitions = partitions;
        }

        public void setError(ConsumerProtocol.AssignmentError error) {
            this.error = error;
        }

        public ByteBuffer userData() {
            return userData;
        }

        @Override
        public String toString() {
            return "Assignment(" +
                "version=" + version +
                ", partitions=" + partitions +
                ", error=" + error +
                ')';
        }
    }

}
