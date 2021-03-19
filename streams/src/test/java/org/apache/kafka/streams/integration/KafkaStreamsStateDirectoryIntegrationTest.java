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
package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreamsWrapper;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.kafka.streams.processor.internals.StreamThread.State;
import org.apache.kafka.test.TestUtils;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.apache.kafka.test.TestUtils.waitForCondition;

public class KafkaStreamsStateDirectoryIntegrationTest {
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    @BeforeClass
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterClass
    public static void closeCluster() {
        CLUSTER.stop();
    }

    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(TestUtils.tempDirectory());

    @Rule
    public final TestName testName = new TestName();

    private static final String APP_ID_PREFIX = "state-directory-integration-test";
    private static final String INPUT_TOPIC = "inputTopic";
    private static final String OUTPUT_TOPIC = "outputTopic";
    private static final Long COMMIT_INTERVAL = 100L;
    private static final long CLEANUP_DELAY_MS = 100L;

    private KafkaStreams streams;
    private final Properties streamsConfig = new Properties();

    @Before
    public void setup() {
        IntegrationTestUtils.cleanStateBeforeTest(CLUSTER, 2, INPUT_TOPIC, OUTPUT_TOPIC);

        final String safeTestName = safeUniqueTestName(getClass(), testName);

        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID_PREFIX + safeTestName);
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, testFolder.getRoot().getPath());
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL);
        streamsConfig.put(StreamsConfig.STATE_CLEANUP_DELAY_MS_CONFIG, CLEANUP_DELAY_MS);
    }

    @After
    public void cleanup() throws InterruptedException {
        CLUSTER.deleteAllTopicsAndWait(120000);
        if (streams != null) {
            streams.close();
        }
    }

    @Test
    public void cleanerThreadShouldNotWipeOutOwnedTaskDirectories() throws Exception {
        streamsConfig.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
        streamsConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 2);
        streamsConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 6);

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Long, String> stream = builder.stream(INPUT_TOPIC);
        stream
            .groupByKey()
            .aggregate(() -> "", (k, v, a) -> a + v)
            .toStream().to(OUTPUT_TOPIC);

        streams = new KafkaStreamsWrapper(builder.build(), streamsConfig);
        streams.setUncaughtExceptionHandler((t) -> StreamThreadExceptionResponse.REPLACE_THREAD);

        final List<StreamThread> initialThreads = new ArrayList<>(((KafkaStreamsWrapper) streams).threads());

        // Use the state listener to force this thread to shutdown once it reaches RUNNING, and to interrupt
        // the shutdown so that it doesn't get to clean up and leaves orphaned task directories
        StreamThread interruptedThread = initialThreads.get(0);
        final  AtomicBoolean interruptedThreadReachedRunning = new AtomicBoolean(false);
        interruptedThread.setStateListener((t, newState, oldState) -> {
            if (newState == State.RUNNING) {
                throw new IllegalStateException("Trigger a shutdown");
            } else if (newState == State.PENDING_SHUTDOWN) {
                throw new IllegalStateException("Interrupt the shutdown!");
            }
        });

        streams.start();
        waitForCondition(interruptedThreadReachedRunning::get, "Thread to interrupt never reached RUNNING");

        // the remaining threads will end up with the killed thread's orphaned tasks, and won't be able to initialize
        // and get back to RUNNING unless the cleanup thread has correctly identified and terminated

        // wait for the thread to be killed, then wait for its replacement to be started up and to reach RUNNING
        waitForCondition(() -> !interruptedThread.isAlive(), "Killed thread hasn't stopped running yet");
        waitForCondition(() -> {
            int numRunningThreads = 0;
            for (final StreamThread thread : new ArrayList<>(((KafkaStreamsWrapper) streams).threads())) {
                if (thread.state() == State.RUNNING) {
                    ++numRunningThreads;
                }
            }
            return numRunningThreads == 2;
        }, "");
    }

}
