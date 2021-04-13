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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.HostInfo;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.stream.Collectors;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NAMED_TASK_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NAMED_TASK_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NAMED_TASK_1_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.NAMED_TASK_1_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_1;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.MIN_NAMED_TOPOLOGY_VERSION;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.UNKNOWN;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

@RunWith(Parameterized.class)
public class AssignmentInfoTest {

    private static final TaskId[] ACTIVE_TASKS = new TaskId[]{
        TASK_0_0,
        TASK_0_1,
        TASK_1_0,
        TASK_1_1
    };

    private static final TaskId[] STANDBY_TASKS = new TaskId[]{
        TASK_0_0,
        TASK_0_1,
        TASK_1_0,
        TASK_1_1
    };

    private static final TaskId[] ACTIVE_TASKS_WITH_NAMED_TOPOLOGY = new TaskId[]{
        NAMED_TASK_0_0,
        NAMED_TASK_0_1,
        NAMED_TASK_1_0,
        NAMED_TASK_1_1
    };

    private static final TaskId[] STANDBY_TASKS_WITH_NAMED_TOPOLOGY = new TaskId[]{
        NAMED_TASK_0_0,
        NAMED_TASK_0_1,
        NAMED_TASK_1_0,
        NAMED_TASK_1_1
    };

    @Parameterized.Parameters
    public static Collection<TaskId[][]> data() {
        return asList(new TaskId[][][] {
            {ACTIVE_TASKS, STANDBY_TASKS},
            {ACTIVE_TASKS_WITH_NAMED_TOPOLOGY, STANDBY_TASKS_WITH_NAMED_TOPOLOGY}
        });
    }

    private final List<TaskId> activeTasks;
    private final Map<TaskId, Set<TopicPartition>> standbyTasks;

    public AssignmentInfoTest(TaskId[] activeTasks, TaskId[] standbyTasks) {
        this.activeTasks = Arrays.stream(activeTasks).collect(Collectors.toList());
        this.standbyTasks = new HashMap<>();
        int i = 0;
        for (final TaskId task : standbyTasks) {
            this.standbyTasks.put(task, mkSet(new TopicPartition("topic1", i), new TopicPartition("topic2", i)));
            ++i;
        }
    }

    private final Map<HostInfo, Set<TopicPartition>> activeAssignment = mkMap(
        mkEntry(new HostInfo("localhost", 8088),
            mkSet(new TopicPartition("t0", 0),
                new TopicPartition("t1", 0),
                new TopicPartition("t2", 0))),
        mkEntry(new HostInfo("localhost", 8089),
            mkSet(new TopicPartition("t0", 1),
                new TopicPartition("t1", 1),
                new TopicPartition("t2", 1)))
    );

    private final Map<HostInfo, Set<TopicPartition>> standbyAssignment = mkMap(
        mkEntry(new HostInfo("localhost", 8088),
            mkSet(new TopicPartition("t1", 0),
                new TopicPartition("t2", 0))),
        mkEntry(new HostInfo("localhost", 8089),
            mkSet(new TopicPartition("t1", 1),
                new TopicPartition("t2", 1)))
    );

    @Test
    public void shouldUseLatestSupportedVersionByDefault() {
        final AssignmentInfo info = new AssignmentInfo(LATEST_SUPPORTED_VERSION, activeTasks, standbyTasks, activeAssignment, standbyAssignment, 0);
        assertEquals(LATEST_SUPPORTED_VERSION, info.version());
    }

    @Test
    public void shouldThrowForUnknownVersion1() {
        assertThrows(IllegalArgumentException.class, () -> new AssignmentInfo(0, activeTasks, standbyTasks,
            activeAssignment, Collections.emptyMap(), 0));
    }

    @Test
    public void shouldThrowForUnknownVersion2() {
        assertThrows(IllegalArgumentException.class, () -> new AssignmentInfo(LATEST_SUPPORTED_VERSION + 1,
            activeTasks, standbyTasks, activeAssignment, Collections.emptyMap(), 0));
    }

    @Test
    public void shouldEncodeAndDecodeVersion1() {
        final int version = 1;
        final AssignmentInfo info = new AssignmentInfo(version, activeTasks, standbyTasks, activeAssignment, standbyAssignment, 0);
        final AssignmentInfo expectedInfo = new AssignmentInfo(version, UNKNOWN, getActiveTaskListForVersion(version), arrayToStandbyTaskMap(version), Collections.emptyMap(), Collections.emptyMap(), 0);
        assertEquals(expectedInfo, AssignmentInfo.decode(info.encode()));
    }

    @Test
    public void shouldEncodeAndDecodeVersion2() {
        final int version = 2;
        final AssignmentInfo info = new AssignmentInfo(version, activeTasks, standbyTasks, activeAssignment, standbyAssignment, 0);
        final AssignmentInfo expectedInfo =
            new AssignmentInfo(version, UNKNOWN, getActiveTaskListForVersion(version), arrayToStandbyTaskMap(version), activeAssignment, Collections.emptyMap(), 0);
        assertEquals(expectedInfo, AssignmentInfo.decode(info.encode()));
    }

    @Test
    public void shouldEncodeAndDecodeVersion3() {
        final int version = 3;
        final AssignmentInfo info = new AssignmentInfo(version, activeTasks, standbyTasks, activeAssignment, standbyAssignment, 0);
        final AssignmentInfo expectedInfo =
            new AssignmentInfo(version, LATEST_SUPPORTED_VERSION, getActiveTaskListForVersion(version),arrayToStandbyTaskMap(version), activeAssignment, Collections.emptyMap(), 0);
        assertEquals(expectedInfo, AssignmentInfo.decode(info.encode()));
    }

    @Test
    public void shouldEncodeAndDecodeVersion4() {
        final int version = 4;
        final AssignmentInfo info = new AssignmentInfo(version, activeTasks, standbyTasks, activeAssignment, standbyAssignment, 2);
        final AssignmentInfo expectedInfo =
            new AssignmentInfo(version, LATEST_SUPPORTED_VERSION, getActiveTaskListForVersion(version), arrayToStandbyTaskMap(version), activeAssignment, Collections.emptyMap(), 2);
        assertEquals(expectedInfo, AssignmentInfo.decode(info.encode()));
    }

    @Test
    public void shouldEncodeAndDecodeVersion5() {
        final int version = 5;
        final AssignmentInfo info = new AssignmentInfo(version, activeTasks, standbyTasks, activeAssignment, standbyAssignment, 2);
        final AssignmentInfo expectedInfo =
            new AssignmentInfo(version, LATEST_SUPPORTED_VERSION, getActiveTaskListForVersion(version), arrayToStandbyTaskMap(version), activeAssignment, Collections.emptyMap(), 2);
        assertEquals(expectedInfo, AssignmentInfo.decode(info.encode()));
    }

    @Test
    public void shouldEncodeAndDecodeVersion6() {
        final int version = 6;
        final AssignmentInfo info = new AssignmentInfo(version, activeTasks, standbyTasks, activeAssignment, standbyAssignment, 2);
        final AssignmentInfo expectedInfo =
            new AssignmentInfo(version, LATEST_SUPPORTED_VERSION, getActiveTaskListForVersion(version), arrayToStandbyTaskMap(version), activeAssignment, standbyAssignment, 2);
        assertEquals(expectedInfo, AssignmentInfo.decode(info.encode()));
    }

    @Test
    public void shouldEncodeAndDecodeVersion7() {
        final int version = 7;
        final AssignmentInfo info =
            new AssignmentInfo(version, activeTasks, standbyTasks, activeAssignment, standbyAssignment, 2);
        final AssignmentInfo expectedInfo =
            new AssignmentInfo(version, LATEST_SUPPORTED_VERSION, getActiveTaskListForVersion(version), arrayToStandbyTaskMap(version), activeAssignment, standbyAssignment, 2);
        assertEquals(expectedInfo, AssignmentInfo.decode(info.encode()));
    }

    @Test
    public void shouldEncodeAndDecodeVersion8() {
        final int version = 8;
        final AssignmentInfo info =
            new AssignmentInfo(version, activeTasks, standbyTasks, activeAssignment, standbyAssignment, 2);
        final AssignmentInfo expectedInfo =
            new AssignmentInfo(version, LATEST_SUPPORTED_VERSION, getActiveTaskListForVersion(version), arrayToStandbyTaskMap(version), activeAssignment, standbyAssignment, 2);
        assertEquals(expectedInfo, AssignmentInfo.decode(info.encode()));
    }

    @Test
    public void shouldEncodeAndDecodeVersion9() {
        final int version = 9;
        final AssignmentInfo info =
            new AssignmentInfo(version, activeTasks, standbyTasks, activeAssignment, standbyAssignment, 2);
        final AssignmentInfo expectedInfo =
            new AssignmentInfo(version, LATEST_SUPPORTED_VERSION, getActiveTaskListForVersion(version), arrayToStandbyTaskMap(version), activeAssignment, standbyAssignment, 2);
        assertEquals(expectedInfo, AssignmentInfo.decode(info.encode()));
    }

    @Test
    public void shouldEncodeAndDecodeVersion10() {
        final int version = 10;
        final AssignmentInfo info =
            new AssignmentInfo(version, activeTasks, standbyTasks, activeAssignment, standbyAssignment, 2);
        final AssignmentInfo expectedInfo =
            new AssignmentInfo(version, LATEST_SUPPORTED_VERSION, getActiveTaskListForVersion(version), arrayToStandbyTaskMap(version), activeAssignment, standbyAssignment, 2);
        assertEquals(expectedInfo, AssignmentInfo.decode(info.encode()));
    }

    @Test
    public void shouldEncodeAndDecodeSmallerCommonlySupportedVersion() {
        final int usedVersion = 5;
        final int commonlySupportedVersion = 5;
        final AssignmentInfo info = new AssignmentInfo(usedVersion, commonlySupportedVersion, activeTasks, standbyTasks,
            activeAssignment, standbyAssignment, 2);
        final AssignmentInfo expectedInfo =
            new AssignmentInfo(usedVersion, commonlySupportedVersion, getActiveTaskListForVersion(usedVersion), arrayToStandbyTaskMap(usedVersion), activeAssignment, Collections.emptyMap(), 2);
        assertEquals(expectedInfo, AssignmentInfo.decode(info.encode()));
    }

    @Test
    public void nextRebalanceTimeShouldBeMaxValueByDefault() {
        final int version = 7;
        final AssignmentInfo info = new AssignmentInfo(version, getActiveTaskListForVersion(version), arrayToStandbyTaskMap(version), activeAssignment, standbyAssignment, 0);
        assertEquals(info.nextRebalanceMs(), Long.MAX_VALUE);
    }

    @Test
    public void shouldDecodeDefaultNextRebalanceTime() {
        final int version = 7;
        final AssignmentInfo info = new AssignmentInfo(version, getActiveTaskListForVersion(version), arrayToStandbyTaskMap(version), activeAssignment, standbyAssignment, 0);
        assertEquals(info.nextRebalanceMs(), Long.MAX_VALUE);
    }

    @Test
    public void shouldEncodeAndDecodeNextRebalanceTime() {
        final int version = 7;
        final AssignmentInfo info = new AssignmentInfo(version, getActiveTaskListForVersion(version), arrayToStandbyTaskMap(version), activeAssignment, standbyAssignment, 0);
        info.setNextRebalanceTime(1000L);
        assertEquals(1000L, AssignmentInfo.decode(info.encode()).nextRebalanceMs());
    }

    /**
     * @return the list of active tasks with or without the named topology depending on the version used to encode
     */
    private List<TaskId> getActiveTaskListForVersion(final int version) {
        if (version >= MIN_NAMED_TOPOLOGY_VERSION) {
            return activeTasks;
        } else {
            return Arrays.stream(ACTIVE_TASKS).collect(Collectors.toList());
        }
    }

    /**
     * @return the map of standby tasks with or without the named topology depending on the version used to encode
     */
    private Map<TaskId, Set<TopicPartition>> arrayToStandbyTaskMap(final int version) {
        if (version >= MIN_NAMED_TOPOLOGY_VERSION) {
            return standbyTasks;
        } else {
            final Map<TaskId, Set<TopicPartition>> standbyTasksWithoutNamedTopology = new HashMap<>();
            int i = 0;
            for (final TaskId task : STANDBY_TASKS) {
                standbyTasksWithoutNamedTopology.put(task, mkSet(new TopicPartition("topic1", i), new TopicPartition("topic2", i)));
                ++i;
            }
            return standbyTasksWithoutNamedTopology;
        }
    }


}
