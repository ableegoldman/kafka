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

import java.time.Instant;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.test.TestRecord;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.test.StreamsTestUtils.startKafkaStreamsAndWaitForRunningState;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests all available joins of Kafka Streams DSL.
 */
@Category({IntegrationTest.class})
@RunWith(value = Parameterized.class)
public abstract class AbstractJoinIntegrationTest {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(TestUtils.tempDirectory());

    @Parameterized.Parameters(name = "caching enabled = {0}")
    public static Collection<Object[]> data() {
        final List<Object[]> values = new ArrayList<>();
        for (final boolean cacheEnabled : Arrays.asList(true, false)) {
            values.add(new Object[]{cacheEnabled});
        }
        return values;
    }

    static String appID;

    private static final Long COMMIT_INTERVAL = 100L;
    static final Properties STREAMS_CONFIG = new Properties();
    static final String INPUT_TOPIC_RIGHT = "inputTopicRight";
    static final String INPUT_TOPIC_LEFT = "inputTopicLeft";
    static final String OUTPUT_TOPIC = "outputTopic";
    static final long ANY_UNIQUE_KEY = 0L;

    private KafkaStreams streams;
    private TopologyTestDriver topologyTestDriver;

    StreamsBuilder builder;
    int numRecordsExpected = 0;
    AtomicBoolean finalResultReached = new AtomicBoolean(false);

    private final List<Input<String>> input = Arrays.asList(
        new Input<>(INPUT_TOPIC_LEFT, null),
        new Input<>(INPUT_TOPIC_RIGHT, null),
        new Input<>(INPUT_TOPIC_LEFT, "A"),
        new Input<>(INPUT_TOPIC_RIGHT, "a"),
        new Input<>(INPUT_TOPIC_LEFT, "B"),
        new Input<>(INPUT_TOPIC_RIGHT, "b"),
        new Input<>(INPUT_TOPIC_LEFT, null),
        new Input<>(INPUT_TOPIC_RIGHT, null),
        new Input<>(INPUT_TOPIC_LEFT, "C"),
        new Input<>(INPUT_TOPIC_RIGHT, "c"),
        new Input<>(INPUT_TOPIC_RIGHT, null),
        new Input<>(INPUT_TOPIC_LEFT, null),
        new Input<>(INPUT_TOPIC_RIGHT, null),
        new Input<>(INPUT_TOPIC_RIGHT, "d"),
        new Input<>(INPUT_TOPIC_LEFT, "D")
    );

    final ValueJoiner<String, String, String> valueJoiner = (value1, value2) -> value1 + "-" + value2;

    final boolean cacheEnabled;

    private static final long TIMEOUT = 30000;

    AbstractJoinIntegrationTest(final boolean cacheEnabled) {
        this.cacheEnabled = cacheEnabled;
    }

    @BeforeClass
    public static void setupConfigsAndUtils() {
        STREAMS_CONFIG.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        STREAMS_CONFIG.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy-brokers");
        STREAMS_CONFIG.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        STREAMS_CONFIG.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        STREAMS_CONFIG.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL);
    }

    void prepareEnvironment() {
        if (!cacheEnabled) {
            STREAMS_CONFIG.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        }

        STREAMS_CONFIG.put(StreamsConfig.STATE_DIR_CONFIG, testFolder.getRoot().getPath());
    }

    @After
    public void cleanup()  {
        try {
            topologyTestDriver.close();
        } catch (final RuntimeException e) {
            log.warn("The following exception was thrown while trying to close the TopologyTestDriver (note that " +
                         "KAFKA-6647 causes this when running on Windows):", e);
        }
    }

    private void checkResult(final String outputTopic, final List<KeyValueTimestamp<Long, String>> expectedResult) {
        final TestOutputTopic<Long, String> testOutputTopic = topologyTestDriver.createOutputTopic(outputTopic, new LongDeserializer(), new StringDeserializer());

        for (final KeyValueTimestamp<Long, String> expected : expectedResult) {
            assertFalse(testOutputTopic.isEmpty());
            final TestRecord<Long, String> record = testOutputTopic.readRecord();
            assertEquals(expected.key(), record.key());
            assertEquals(expected.value(), record.value());
            assertEquals((Long) expected.timestamp(), record.timestamp());
        }
        assertTrue(testOutputTopic.isEmpty());
    }

    private void checkResult(final String outputTopic, final KeyValueTimestamp<Long, String> expectedFinalResult, final int expectedTotalNumRecords) {
        final List<KeyValueTimestamp<Long, String>> result =
            IntegrationTestUtils.waitUntilMinKeyValueWithTimestampRecordsReceived(RESULT_CONSUMER_CONFIG, outputTopic, expectedTotalNumRecords, 30 * 1000L);
        assertThat(result.get(result.size() - 1), is(expectedFinalResult));
    }

    /*
     * Runs the actual test. Checks the result after each input record to ensure fixed processing order.
     * If an input tuple does not trigger any result, "expectedResult" should contain a "null" entry
     */
    void runTest(final List<List<KeyValueTimestamp<Long, String>>> expectedResult) throws Exception {
        runTest(expectedResult, null);
    }

    /*
     * Runs the actual test. Checks the result after each input record to ensure fixed processing order.
     * If an input tuple does not trigger any result, "expectedResult" should contain a "null" entry
     */
    void runTest(final List<List<KeyValueTimestamp<Long, String>>> expectedResult, final String storeName) throws Exception {
        assert expectedResult.size() == input.size();

        IntegrationTestUtils.purgeLocalStreamsState(STREAMS_CONFIG);

        topologyTestDriver = new TopologyTestDriver(builder.build(STREAMS_CONFIG), STREAMS_CONFIG);
        streams = new KafkaStreams(builder.build(), STREAMS_CONFIG);

        KeyValueTimestamp<Long, String> expectedFinalResult = null;

        final long firstTimestamp = System.currentTimeMillis();
        long ts = firstTimestamp;

        final Iterator<List<KeyValueTimestamp<Long, String>>> resultIterator = expectedResult.iterator();
        for (final Input<String> singleInput : input) {
            final TestInputTopic<Long, String> inputTopic =
                topologyTestDriver.createInputTopic(singleInput.topic, new LongSerializer(), new StringSerializer());
            inputTopic.pipeInput(new TestRecord<>(singleInput.record.key, singleInput.record.value, Instant.ofEpochMilli(++ts)));

            final List<KeyValueTimestamp<Long, String>> expected = resultIterator.next();

            if (expected != null) {
                final List<KeyValueTimestamp<Long, String>> updatedExpected = new LinkedList<>();
                for (final KeyValueTimestamp<Long, String> record : expected) {
                    updatedExpected.add(new KeyValueTimestamp<>(record.key(), record.value(), firstTimestamp + record.timestamp()));
                }

                checkResult(OUTPUT_TOPIC, updatedExpected);
                expectedFinalResult = updatedExpected.get(expected.size() - 1);
            }


            if (storeName != null) {
                checkQueryableStore(storeName, expectedFinalResult);
            }
        }

        streams.close();
    }

    /*
     * Runs the actual test. Checks the final result only after expected number of records have been consumed.
     */
    void runTest(final KeyValueTimestamp<Long, String> expectedFinalResult, final String storeName) throws Exception {
        IntegrationTestUtils.purgeLocalStreamsState(STREAMS_CONFIG);
        streams = new KafkaStreams(builder.build(), STREAMS_CONFIG);

        try {
            startKafkaStreamsAndWaitForRunningState(streams, TIMEOUT);

            final long firstTimestamp = System.currentTimeMillis();
            long ts = firstTimestamp;

            for (final Input<String> singleInput : input) {
                producer.send(new ProducerRecord<>(singleInput.topic, null, ++ts, singleInput.record.key, singleInput.record.value)).get();
            }

            TestUtils.waitForCondition(() -> finalResultReached.get(), "Never received expected final result.");

            final KeyValueTimestamp<Long, String> updatedExpectedFinalResult =
                new KeyValueTimestamp<>(
                    expectedFinalResult.key(),
                    expectedFinalResult.value(),
                    firstTimestamp + expectedFinalResult.timestamp());
            checkResult(OUTPUT_TOPIC, updatedExpectedFinalResult, numRecordsExpected);

            if (storeName != null) {
                checkQueryableStore(storeName, updatedExpectedFinalResult);
            }
        } finally {
            streams.close();
        }
    }

    /*
     * Checks the embedded queryable state store snapshot
     */
    private void checkQueryableStore(final String queryableName, final KeyValueTimestamp<Long, String> expectedFinalResult) {
        final ReadOnlyKeyValueStore<Long, ValueAndTimestamp<String>> store = streams.store(queryableName, QueryableStoreTypes.timestampedKeyValueStore());

        final KeyValueIterator<Long, ValueAndTimestamp<String>> all = store.all();
        final KeyValue<Long, ValueAndTimestamp<String>> onlyEntry = all.next();

        try {
            assertThat(onlyEntry.key, is(expectedFinalResult.key()));
            assertThat(onlyEntry.value.value(), is(expectedFinalResult.value()));
            assertThat(onlyEntry.value.timestamp(), is(expectedFinalResult.timestamp()));
            assertThat(all.hasNext(), is(false));
        } finally {
            all.close();
        }
    }

    private static final class Input<V> {
        String topic;
        KeyValue<Long, V> record;

        Input(final String topic, final V value) {
            this.topic = topic;
            record = KeyValue.pair(ANY_UNIQUE_KEY, value);
        }
    }
}
