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
package org.apache.kafka.streams.processor.internals.assignment;

import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.Task;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Test;

import java.nio.ByteBuffer;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.union;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NAMED_TASK_T0_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NAMED_TASK_T0_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NAMED_TASK_T0_1_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NAMED_TASK_T0_1_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NAMED_TASK_T1_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NAMED_TASK_T1_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NAMED_TASK_T1_0_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NAMED_TASK_T2_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NAMED_TASK_T2_1_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NAMED_TASK_T2_2_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_3;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_2_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_2_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_1;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION;
import static org.apache.kafka.streams.processor.internals.assignment.SubscriptionInfo.MIN_VERSION_OFFSET_SUM_SUBSCRIPTION;
import static org.apache.kafka.streams.processor.internals.assignment.SubscriptionInfo.UNKNOWN_OFFSET_SUM;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class SubscriptionInfoTest {
    private static final TaskId[] ACTIVE_TASKS = new TaskId[]{
        TASK_0_0,
        TASK_0_2,
        TASK_1_0,
        TASK_1_2,
        TASK_2_1
    };

    private static final TaskId[] STANDBY_TASKS = new TaskId[]{
        TASK_0_1,
        TASK_0_3,
        TASK_1_1,
        TASK_2_0,
        TASK_0_2
    };

    private static final TaskId[] ACTIVE_TASKS_WITH_NAMED_TOPOLOGY = new TaskId[]{
        NAMED_TASK_T0_0_0,
        NAMED_TASK_T0_1_0,
        NAMED_TASK_T1_0_0,
        NAMED_TASK_T1_0_2,
        NAMED_TASK_T2_0_0
    };

    private static final TaskId[] STANDBY_TASKS_WITH_NAMED_TOPOLOGY = new TaskId[]{
        NAMED_TASK_T0_0_1,
        NAMED_TASK_T0_1_1,
        NAMED_TASK_T1_0_1,
        NAMED_TASK_T2_1_0,
        NAMED_TASK_T2_2_0
    };

    private final static String IGNORED_USER_ENDPOINT = "ignoredUserEndpoint:80";
    private static final byte IGNORED_UNIQUE_FIELD = (byte) 0;
    private static final int IGNORED_ERROR_CODE = 0;

    private static final List<Long> offsetSums = asList(
        Task.LATEST_OFFSET, Task.LATEST_OFFSET, 0L, 10L, Task.LATEST_OFFSET, 0L, 0L, 20L, 500L, 10000L);

    @Parameterized.Parameters
    public static Collection<TaskId[][]> data() {
        return asList(new TaskId[][][] {
                {ACTIVE_TASKS, STANDBY_TASKS},
                {ACTIVE_TASKS_WITH_NAMED_TOPOLOGY, STANDBY_TASKS_WITH_NAMED_TOPOLOGY}
        });
    }

    private final Set<TaskId> activeTasks;
    private final Set<TaskId> standbyTasks;
    private final Map<TaskId, Long> taskOffsetSums;

    public SubscriptionInfoTest(final TaskId[] activeTasks, final TaskId[] standbyTasks) {
        this.activeTasks = Arrays.stream(activeTasks).collect(Collectors.toSet());
        this.standbyTasks = Arrays.stream(standbyTasks).collect(Collectors.toSet());
        this.taskOffsetSums = new HashMap<>();
        // list of the offset sums for the 10 total tasks in the active and standby lists
        final Iterator<Long> offsetIterator = offsetSums.iterator();
        for (final TaskId task : union(HashSet::new, this.activeTasks, this.standbyTasks)) {
            this.taskOffsetSums.put(task, offsetIterator.next());
        }
    }

    @Test
    public void shouldThrowForUnknownVersion1() {
        assertThrows(IllegalArgumentException.class, () -> new SubscriptionInfo(
            0,
            LATEST_SUPPORTED_VERSION,
            UUID_1,
            "localhost:80",
                taskOffsetSums,
            IGNORED_UNIQUE_FIELD,
            IGNORED_ERROR_CODE
        ));
    }

    @Test
    public void shouldThrowForUnknownVersion2() {
        assertThrows(IllegalArgumentException.class, () -> new SubscriptionInfo(
            LATEST_SUPPORTED_VERSION + 1,
            LATEST_SUPPORTED_VERSION,
            UUID_1,
            "localhost:80",
                taskOffsetSums,
            IGNORED_UNIQUE_FIELD,
            IGNORED_ERROR_CODE
        ));
    }

    @Test
    public void shouldEncodeAndDecodeVersion1() {
        final SubscriptionInfo info = new SubscriptionInfo(
            1,
            LATEST_SUPPORTED_VERSION,
            UUID_1,
            IGNORED_USER_ENDPOINT,
                taskOffsetSums,
            IGNORED_UNIQUE_FIELD,
            IGNORED_ERROR_CODE
        );
        final SubscriptionInfo decoded = SubscriptionInfo.decode(info.encode());
        assertEquals(1, decoded.version());
        assertEquals(SubscriptionInfo.UNKNOWN, decoded.latestSupportedVersion());
        assertEquals(UUID_1, decoded.processId());
        assertEquals(activeTasks, decoded.prevTasks());
        assertEquals(standbyTasks, decoded.standbyTasks());
        assertNull(decoded.userEndPoint());
    }

    @Test
    public void generatedVersion1ShouldBeDecodableByLegacyLogic() {
        final SubscriptionInfo info = new SubscriptionInfo(
            1,
            1234,
            UUID_1,
            "ignoreme",
                taskOffsetSums,
            IGNORED_UNIQUE_FIELD,
            IGNORED_ERROR_CODE
        );
        final ByteBuffer buffer = info.encode();

        final LegacySubscriptionInfoSerde decoded = LegacySubscriptionInfoSerde.decode(buffer);
        assertEquals(1, decoded.version());
        assertEquals(SubscriptionInfo.UNKNOWN, decoded.latestSupportedVersion());
        assertEquals(UUID_1, decoded.processId());
        assertEquals(activeTasks, decoded.prevTasks());
        assertEquals(standbyTasks, decoded.standbyTasks());
        assertNull(decoded.userEndPoint());
    }

    @Test
    public void generatedVersion1ShouldDecodeLegacyFormat() {
        final LegacySubscriptionInfoSerde info = new LegacySubscriptionInfoSerde(
            1,
            LATEST_SUPPORTED_VERSION,
            UUID_1,
                activeTasks,
                standbyTasks,
            "localhost:80"
        );
        final ByteBuffer buffer = info.encode();
        buffer.rewind();
        final SubscriptionInfo decoded = SubscriptionInfo.decode(buffer);
        assertEquals(1, decoded.version());
        assertEquals(SubscriptionInfo.UNKNOWN, decoded.latestSupportedVersion());
        assertEquals(UUID_1, decoded.processId());
        assertEquals(activeTasks, decoded.prevTasks());
        assertEquals(standbyTasks, decoded.standbyTasks());
        assertNull(decoded.userEndPoint());
    }

    @Test
    public void shouldEncodeAndDecodeVersion2() {
        final SubscriptionInfo info = new SubscriptionInfo(
            2,
            LATEST_SUPPORTED_VERSION,
            UUID_1,
            "localhost:80",
                taskOffsetSums,
            IGNORED_UNIQUE_FIELD,
            IGNORED_ERROR_CODE
        );
        final SubscriptionInfo decoded = SubscriptionInfo.decode(info.encode());
        assertEquals(2, decoded.version());
        assertEquals(SubscriptionInfo.UNKNOWN, decoded.latestSupportedVersion());
        assertEquals(UUID_1, decoded.processId());
        assertEquals(activeTasks, decoded.prevTasks());
        assertEquals(standbyTasks, decoded.standbyTasks());
        assertEquals("localhost:80", decoded.userEndPoint());
    }

    @Test
    public void generatedVersion2ShouldBeDecodableByLegacyLogic() {
        final SubscriptionInfo info = new SubscriptionInfo(
            2,
            LATEST_SUPPORTED_VERSION,
            UUID_1,
            "localhost:80",
                taskOffsetSums,
            IGNORED_UNIQUE_FIELD,
            IGNORED_ERROR_CODE
        );
        final ByteBuffer buffer = info.encode();

        final LegacySubscriptionInfoSerde decoded = LegacySubscriptionInfoSerde.decode(buffer);
        assertEquals(2, decoded.version());
        assertEquals(SubscriptionInfo.UNKNOWN, decoded.latestSupportedVersion());
        assertEquals(UUID_1, decoded.processId());
        assertEquals(activeTasks, decoded.prevTasks());
        assertEquals(standbyTasks, decoded.standbyTasks());
        assertEquals("localhost:80", decoded.userEndPoint());
    }

    @Test
    public void generatedVersion2ShouldDecodeLegacyFormat() {
        final LegacySubscriptionInfoSerde info = new LegacySubscriptionInfoSerde(
            2,
            LATEST_SUPPORTED_VERSION,
            UUID_1,
                activeTasks,
                standbyTasks,
            "localhost:80"
        );
        final ByteBuffer buffer = info.encode();
        buffer.rewind();
        final SubscriptionInfo decoded = SubscriptionInfo.decode(buffer);
        assertEquals(2, decoded.version());
        assertEquals(SubscriptionInfo.UNKNOWN, decoded.latestSupportedVersion());
        assertEquals(UUID_1, decoded.processId());
        assertEquals(activeTasks, decoded.prevTasks());
        assertEquals(standbyTasks, decoded.standbyTasks());
        assertEquals("localhost:80", decoded.userEndPoint());
    }

    @Test
    public void shouldEncodeAndDecodeVersion3And4() {
        for (int version = 3; version <= 4; version++) {
            final SubscriptionInfo info = new SubscriptionInfo(
                version,
                LATEST_SUPPORTED_VERSION,
                UUID_1,
                "localhost:80",
                    taskOffsetSums,
                IGNORED_UNIQUE_FIELD,
                IGNORED_ERROR_CODE
            );
            final SubscriptionInfo decoded = SubscriptionInfo.decode(info.encode());
            assertEquals(version, decoded.version());
            assertEquals(LATEST_SUPPORTED_VERSION, decoded.latestSupportedVersion());
            assertEquals(UUID_1, decoded.processId());
            assertEquals(activeTasks, decoded.prevTasks());
            assertEquals(standbyTasks, decoded.standbyTasks());
            assertEquals("localhost:80", decoded.userEndPoint());
        }
    }

    @Test
    public void generatedVersion3And4ShouldBeDecodableByLegacyLogic() {
        for (int version = 3; version <= 4; version++) {
            final SubscriptionInfo info = new SubscriptionInfo(
                version,
                LATEST_SUPPORTED_VERSION,
                UUID_1,
                "localhost:80",
                    taskOffsetSums,
                IGNORED_UNIQUE_FIELD,
                IGNORED_ERROR_CODE
            );
            final ByteBuffer buffer = info.encode();

            final LegacySubscriptionInfoSerde decoded = LegacySubscriptionInfoSerde.decode(buffer);
            assertEquals(version, decoded.version());
            assertEquals(LATEST_SUPPORTED_VERSION, decoded.latestSupportedVersion());
            assertEquals(UUID_1, decoded.processId());
            assertEquals(activeTasks, decoded.prevTasks());
            assertEquals(standbyTasks, decoded.standbyTasks());
            assertEquals("localhost:80", decoded.userEndPoint());
        }
    }

    @Test
    public void generatedVersion3To6ShouldDecodeLegacyFormat() {
        for (int version = 3; version <= 6; version++) {
            final LegacySubscriptionInfoSerde info = new LegacySubscriptionInfoSerde(
                version,
                LATEST_SUPPORTED_VERSION,
                UUID_1,
                    activeTasks,
                    standbyTasks,
                "localhost:80"
            );
            final ByteBuffer buffer = info.encode();
            buffer.rewind();
            final SubscriptionInfo decoded = SubscriptionInfo.decode(buffer);
            final String message = "for version: " + version;
            assertEquals(message, version, decoded.version());
            assertEquals(message, LATEST_SUPPORTED_VERSION, decoded.latestSupportedVersion());
            assertEquals(message, UUID_1, decoded.processId());
            assertEquals(message, activeTasks, decoded.prevTasks());
            assertEquals(message, standbyTasks, decoded.standbyTasks());
            assertEquals(message, "localhost:80", decoded.userEndPoint());
        }
    }

    @Test
    public void shouldEncodeAndDecodeVersion5() {
        final SubscriptionInfo info =
            new SubscriptionInfo(5, LATEST_SUPPORTED_VERSION, UUID_1, "localhost:80", taskOffsetSums, IGNORED_UNIQUE_FIELD, IGNORED_ERROR_CODE);
        assertEquals(info, SubscriptionInfo.decode(info.encode()));
    }

    @Test
    public void shouldAllowToDecodeFutureSupportedVersion() {
        final SubscriptionInfo info = SubscriptionInfo.decode(encodeFutureVersion());
        assertEquals(LATEST_SUPPORTED_VERSION + 1, info.version());
        assertEquals(LATEST_SUPPORTED_VERSION + 1, info.latestSupportedVersion());
    }

    @Test
    public void shouldEncodeAndDecodeSmallerLatestSupportedVersion() {
        final int usedVersion = LATEST_SUPPORTED_VERSION - 1;
        final int latestSupportedVersion = LATEST_SUPPORTED_VERSION - 1;

        final SubscriptionInfo info =
            new SubscriptionInfo(usedVersion, latestSupportedVersion, UUID_1, "localhost:80", taskOffsetSums, IGNORED_UNIQUE_FIELD, IGNORED_ERROR_CODE);
        final SubscriptionInfo expectedInfo =
            new SubscriptionInfo(usedVersion, latestSupportedVersion, UUID_1, "localhost:80", taskOffsetSums, IGNORED_UNIQUE_FIELD, IGNORED_ERROR_CODE);
        assertEquals(expectedInfo, SubscriptionInfo.decode(info.encode()));
    }

    @Test
    public void shouldEncodeAndDecodeVersion7() {
        final SubscriptionInfo info =
            new SubscriptionInfo(7, LATEST_SUPPORTED_VERSION, UUID_1, "localhost:80", taskOffsetSums, IGNORED_UNIQUE_FIELD, IGNORED_ERROR_CODE);
        assertThat(info, is(SubscriptionInfo.decode(info.encode())));
    }

    @Test
    public void shouldConvertTaskOffsetSumMapToTaskSets() {
        final SubscriptionInfo info =
            new SubscriptionInfo(7, LATEST_SUPPORTED_VERSION, UUID_1, "localhost:80", taskOffsetSums, IGNORED_UNIQUE_FIELD, IGNORED_ERROR_CODE);
        assertThat(info.prevTasks(), is(activeTasks));
        assertThat(info.standbyTasks(), is(standbyTasks));
    }

    @Test
    public void shouldReturnTaskOffsetSumsMapForDecodedSubscription() {
        final SubscriptionInfo info = SubscriptionInfo.decode(
            new SubscriptionInfo(MIN_VERSION_OFFSET_SUM_SUBSCRIPTION,
                                 LATEST_SUPPORTED_VERSION, UUID_1,
                                 "localhost:80",
                    taskOffsetSums,
                                 IGNORED_UNIQUE_FIELD,
                                 IGNORED_ERROR_CODE
            ).encode());
        assertThat(info.taskOffsetSums(), is(taskOffsetSums));
    }

    @Test
    public void shouldConvertTaskSetsToTaskOffsetSumMapWithOlderSubscription() {
        final Map<TaskId, Long> expectedOffsetSumsMap = mkMap(
            mkEntry(new TaskId(0, 0), Task.LATEST_OFFSET),
            mkEntry(new TaskId(0, 1), Task.LATEST_OFFSET),
            mkEntry(new TaskId(1, 0), Task.LATEST_OFFSET),
            mkEntry(new TaskId(1, 1), UNKNOWN_OFFSET_SUM),
            mkEntry(new TaskId(2, 0), UNKNOWN_OFFSET_SUM)
        );

        final SubscriptionInfo info = SubscriptionInfo.decode(
            new LegacySubscriptionInfoSerde(
                SubscriptionInfo.MIN_VERSION_OFFSET_SUM_SUBSCRIPTION - 1,
                LATEST_SUPPORTED_VERSION,
                UUID_1,
                    activeTasks,
                    standbyTasks,
                "localhost:80")
            .encode());

        assertThat(info.taskOffsetSums(), is(expectedOffsetSumsMap));
    }

    @Test
    public void shouldEncodeAndDecodeVersion8() {
        final SubscriptionInfo info =
            new SubscriptionInfo(8, LATEST_SUPPORTED_VERSION, UUID_1, "localhost:80", taskOffsetSums, IGNORED_UNIQUE_FIELD, IGNORED_ERROR_CODE);
        assertThat(info, is(SubscriptionInfo.decode(info.encode())));
    }

    @Test
    public void shouldNotErrorAccessingFutureVars() {
        final SubscriptionInfo info =
                new SubscriptionInfo(8, LATEST_SUPPORTED_VERSION, UUID_1, "localhost:80", taskOffsetSums, IGNORED_UNIQUE_FIELD, IGNORED_ERROR_CODE);
        try {
            info.errorCode();
        } catch (final Exception e) {
            fail("should not error");
        }
    }

    @Test
    public void shouldEncodeAndDecodeVersion9() {
        final SubscriptionInfo info =
                new SubscriptionInfo(9, LATEST_SUPPORTED_VERSION, UUID_1, "localhost:80", taskOffsetSums, IGNORED_UNIQUE_FIELD, IGNORED_ERROR_CODE);
        assertThat(info, is(SubscriptionInfo.decode(info.encode())));
    }

    @Test
    public void shouldEncodeAndDecodeVersion10() {
        // In version 10 we added a field to the encoded taskID in the offset sum map
        final Map<TaskId, Long> taskOffsetSums = mkMap(
            mkEntry(new TaskId(0, 0), Task.LATEST_OFFSET),
            mkEntry(new TaskId(0, 1), Task.LATEST_OFFSET),
            mkEntry(new TaskId(0, 0), Task.LATEST_OFFSET),
            mkEntry(new TaskId(0, 1), 0L),
            mkEntry(new TaskId(1, 0), 10L)
        );

        final SubscriptionInfo info =
            new SubscriptionInfo(10, LATEST_SUPPORTED_VERSION, UUID_1, "localhost:80", taskOffsetSums, IGNORED_UNIQUE_FIELD, IGNORED_ERROR_CODE);
        assertThat(info, is(SubscriptionInfo.decode(info.encode())));
    }

    @Test
    public void shouldEncodeAndDecodeVersion10WithNamedTopologyTasks() {
        // In version 10 we added a field to the encoded taskID in the offset sum map
        final Map<TaskId, Long> taskOffsetSums = mkMap(
            mkEntry(new TaskId(0, 0, "topology1"), Task.LATEST_OFFSET),
            mkEntry(new TaskId(0, 1, "topology1"), Task.LATEST_OFFSET),
            mkEntry(new TaskId(0, 0, "topology2"), Task.LATEST_OFFSET),
            mkEntry(new TaskId(0, 1, "topology2"), 0L),
            mkEntry(new TaskId(1, 0, "topology2"), 10L)
        );

        final SubscriptionInfo info =
            new SubscriptionInfo(10, LATEST_SUPPORTED_VERSION, UUID_1, "localhost:80", taskOffsetSums, IGNORED_UNIQUE_FIELD, IGNORED_ERROR_CODE);
        assertThat(info, is(SubscriptionInfo.decode(info.encode())));
    }

    private static ByteBuffer encodeFutureVersion() {
        final ByteBuffer buf = ByteBuffer.allocate(4 /* used version */
                                                       + 4 /* supported version */);
        buf.putInt(LATEST_SUPPORTED_VERSION + 1);
        buf.putInt(LATEST_SUPPORTED_VERSION + 1);
        buf.rewind();
        return buf;
    }

}
