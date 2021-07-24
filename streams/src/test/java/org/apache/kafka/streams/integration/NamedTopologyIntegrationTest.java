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
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.apache.kafka.streams.processor.internals.namedtopology.KafkaStreamsNamedTopologyWrapper;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopology;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopologyStreamsBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.utils.UniqueTopicSerdeScope;
import org.apache.kafka.test.TestUtils;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class NamedTopologyIntegrationTest {
    
    // TODO KAFKA-12648:
    //  1) full test coverage for add/removeNamedTopology, covering:
    //      - the "last topology removed" case
    //      - test using multiple clients, with standbys
    //      - test the cleanUpNamedTopology() API


    private static final int NUM_BROKERS = 1;

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    @BeforeClass
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterClass
    public static void closeCluster() {
        CLUSTER.stop();
    }

    @Rule
    public final TestName testName = new TestName();
    private String appId;

    private String inputStream1;
    private String inputStream2;
    private String inputStream3;
    private String outputStream1;
    private String outputStream2;
    private String outputStream3;
    private String storeChangelog1;
    private String storeChangelog2;
    private String storeChangelog3;

    final List<KeyValue<String, Long>> standardInputData = asList(KeyValue.pair("A", 100L), KeyValue.pair("B", 200L), KeyValue.pair("A", 300L), KeyValue.pair("C", 400L));
    final List<KeyValue<String, Long>> standardOutputData = asList(KeyValue.pair("B", 1L), KeyValue.pair("A", 2L), KeyValue.pair("C", 1L)); // output of basic count topology with caching

    final KafkaClientSupplier clientSupplier = new DefaultKafkaClientSupplier();
    final Properties producerConfig = TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, LongSerializer.class);
    final Properties consumerConfig = TestUtils.consumerConfig(CLUSTER.bootstrapServers(), StringDeserializer.class, LongDeserializer.class);

    final NamedTopologyStreamsBuilder topology1Builder = new NamedTopologyStreamsBuilder("topology-1");
    final NamedTopologyStreamsBuilder topology2Builder = new NamedTopologyStreamsBuilder("topology-2");
    final NamedTopologyStreamsBuilder topology3Builder = new NamedTopologyStreamsBuilder("topology-3");

    // builders for the 2nd Streams instance
    final NamedTopologyStreamsBuilder topology1Builder2 = new NamedTopologyStreamsBuilder("topology-1");
    final NamedTopologyStreamsBuilder topology2Builder2 = new NamedTopologyStreamsBuilder("topology-2");
    final NamedTopologyStreamsBuilder topology3Builder2 = new NamedTopologyStreamsBuilder("topology-3");

    Properties props;
    Properties props2;

    KafkaStreamsNamedTopologyWrapper streams;
    KafkaStreamsNamedTopologyWrapper streams2;

    private Properties configProps() {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(appId).getPath());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000L);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return streamsConfiguration;
    }

    @Before
    public void setup() throws InterruptedException {
        appId = safeUniqueTestName(NamedTopologyIntegrationTest.class, testName);
        inputStream1 = appId + "-input-stream-1";
        inputStream2 = appId + "-input-stream-2";
        inputStream3 = appId + "-input-stream-3";
        outputStream1 = appId + "-output-stream-1";
        outputStream2 = appId + "-output-stream-2";
        outputStream3 = appId + "-output-stream-3";
        storeChangelog1 = appId + "-topology-1-store-changelog";
        storeChangelog2 = appId + "-topology-2-store-changelog";
        storeChangelog3 = appId + "-topology-3-store-changelog";
        props = configProps();
        props2 = configProps();
        CLUSTER.createTopic(inputStream1, 2, 1);
        CLUSTER.createTopic(inputStream2, 2, 1);
        CLUSTER.createTopic(inputStream3, 2, 1);
        CLUSTER.createTopic(outputStream1, 2, 1);
        CLUSTER.createTopic(outputStream2, 2, 1);
        CLUSTER.createTopic(outputStream3, 2, 1);
    }

    @After
    public void shutdown() throws Exception {
        if (streams != null) {
            streams.close(Duration.ofSeconds(30));
        }
        if (streams2 != null) {
            streams2.close(Duration.ofSeconds(30));
        }
        CLUSTER.deleteTopics(inputStream1, inputStream2, inputStream3, outputStream1, outputStream2, outputStream3);
    }

    @Test
    public void shouldProcessSingleNamedTopologyAndPrefixInternalTopics() throws Exception {
        produceToInputTopics(inputStream1, standardInputData);
        topology1Builder.stream(inputStream1)
            .selectKey((k, v) -> k)
            .groupByKey()
            .count(Materialized.as(Stores.persistentKeyValueStore("store")))
            .toStream().to(outputStream1);
        streams = new KafkaStreamsNamedTopologyWrapper(topology1Builder.buildNamedTopology(props), props, clientSupplier);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(singletonList(streams), Duration.ofSeconds(15));
        final List<KeyValue<String, Long>> results = waitUntilMinKeyValueRecordsReceived(consumerConfig, outputStream1, 3);
        assertThat(results, equalTo(standardOutputData));

        final Set<String> allTopics = CLUSTER.getAllTopicsInCluster();
        assertThat(allTopics.contains(appId + "-" + "topology-1" + "-store-changelog"), is(true));
        assertThat(allTopics.contains(appId + "-" + "topology-1" + "-store-repartition"), is(true));
    }

    @Test
    public void shouldPrefixAllInternalTopicNamesWithNamedTopology() throws Exception {
        final String countTopologyName = "count-topology";
        final String fkjTopologyName = "FKJ-topology";

        final NamedTopologyStreamsBuilder countBuilder = new NamedTopologyStreamsBuilder(countTopologyName);
        countBuilder.stream(inputStream1).selectKey((k, v) -> k).groupByKey().count();

        final NamedTopologyStreamsBuilder fkjBuilder = new NamedTopologyStreamsBuilder(fkjTopologyName);

        final UniqueTopicSerdeScope serdeScope = new UniqueTopicSerdeScope();
        final KTable<Integer, String> left = fkjBuilder.table(
            inputStream2,
            Consumed.with(serdeScope.decorateSerde(Serdes.Integer(), props, true),
                          serdeScope.decorateSerde(Serdes.String(), props, false))
        );
        final KTable<Integer, String> right = fkjBuilder.table(
            inputStream3,
            Consumed.with(serdeScope.decorateSerde(Serdes.Integer(), props, true),
                          serdeScope.decorateSerde(Serdes.String(), props, false))
        );
        left.join(
            right,
            value -> Integer.parseInt(value.split("\\|")[1]),
            (value1, value2) -> "(" + value1 + "," + value2 + ")",
            Materialized.with(null, serdeScope.decorateSerde(Serdes.String(), props, false)));

        streams = new KafkaStreamsNamedTopologyWrapper(buildNamedTopologies(fkjBuilder, countBuilder), props, clientSupplier);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(singletonList(streams), Duration.ofSeconds(15));

        final String countTopicPrefix = appId + "-" + countTopologyName;
        final String fkjTopicPrefix = appId + "-" + fkjTopologyName;
        final  Set<String> internalTopics = CLUSTER
            .getAllTopicsInCluster().stream()
            .filter(t -> t.endsWith("-repartition") || t.endsWith("-changelog") || t.endsWith("-topic"))
            .collect(Collectors.toSet());
        assertThat(internalTopics, is(mkSet(
            countTopicPrefix + "-KSTREAM-AGGREGATE-STATE-STORE-0000000002-repartition",
            countTopicPrefix + "-KSTREAM-AGGREGATE-STATE-STORE-0000000002-changelog",
            fkjTopicPrefix + "-KTABLE-FK-JOIN-SUBSCRIPTION-REGISTRATION-0000000006-topic",
            fkjTopicPrefix + "-KTABLE-FK-JOIN-SUBSCRIPTION-RESPONSE-0000000014-topic",
            fkjTopicPrefix + "-KTABLE-FK-JOIN-SUBSCRIPTION-STATE-STORE-0000000010-changelog",
            fkjTopicPrefix + "-" + inputStream2 + "-STATE-STORE-0000000000-changelog",
            fkjTopicPrefix + "-" + inputStream3 + "-STATE-STORE-0000000003-changelog"))
        );
    }

    @Test
    public void shouldProcessMultipleIdenticalNamedTopologiesWithInMemoryAndPersistentStateStores() throws Exception {
        produceToInputTopics(inputStream1, standardInputData);
        produceToInputTopics(inputStream2, standardInputData);
        produceToInputTopics(inputStream3, standardInputData);

        topology1Builder.stream(inputStream1).selectKey((k, v) -> k).groupByKey().count(Materialized.as(Stores.persistentKeyValueStore("store"))).toStream().to(outputStream1);
        topology2Builder.stream(inputStream2).selectKey((k, v) -> k).groupByKey().count(Materialized.as(Stores.inMemoryKeyValueStore("store"))).toStream().to(outputStream2);
        topology3Builder.stream(inputStream3).selectKey((k, v) -> k).groupByKey().count(Materialized.as(Stores.persistentKeyValueStore("store"))).toStream().to(outputStream3);
        streams = new KafkaStreamsNamedTopologyWrapper(buildNamedTopologies(topology1Builder, topology2Builder, topology3Builder), props, clientSupplier);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(singletonList(streams), Duration.ofSeconds(15));

        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, outputStream1, 3), equalTo(standardOutputData));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, outputStream2, 3), equalTo(standardOutputData));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, outputStream3, 3), equalTo(standardOutputData));

        assertThat(CLUSTER.getAllTopicsInCluster().containsAll(asList(storeChangelog1, storeChangelog2, storeChangelog3)), is(true));
    }

    @Test
    public void shouldAddNamedTopologyToUnstartedApplicationWithEmptyInitialTopology() throws Exception {
        produceToInputTopics(inputStream1, standardInputData);

        topology1Builder.stream(inputStream1).selectKey((k, v) -> k).groupByKey().count(Materialized.as(Stores.inMemoryKeyValueStore("store"))).toStream().to(outputStream1);
        streams = new KafkaStreamsNamedTopologyWrapper(props, clientSupplier);
        streams.addNamedTopology(topology1Builder.buildNamedTopology(props));
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(singletonList(streams), Duration.ofSeconds(15));

        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, outputStream1, 3), equalTo(standardOutputData));
    }
    
    @Test
    public void shouldAddNamedTopologyToRunningApplicationWithEmptyInitialTopology() throws Exception {
        produceToInputTopics(inputStream1, standardInputData);

        topology1Builder.stream(inputStream1).selectKey((k, v) -> k).groupByKey().count(Materialized.as(Stores.inMemoryKeyValueStore("store"))).toStream().to(outputStream1);
        streams = new KafkaStreamsNamedTopologyWrapper(props, clientSupplier);
        streams.start();

        streams.addNamedTopology(topology1Builder.buildNamedTopology(props));
        IntegrationTestUtils.waitForApplicationState(singletonList(streams), State.RUNNING, Duration.ofSeconds(15));

        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, outputStream1, 3), equalTo(standardOutputData));
    }

    @Test
    public void shouldAddNamedTopologyToRunningApplicationWithSingleInitialNamedTopology() throws Exception {
        produceToInputTopics(inputStream1, standardInputData);
        produceToInputTopics(inputStream2, standardInputData);

        topology1Builder.stream(inputStream1).selectKey((k, v) -> k).groupByKey().count(Materialized.as(Stores.inMemoryKeyValueStore("store"))).toStream().to(outputStream1);
        topology2Builder.stream(inputStream2).selectKey((k, v) -> k).groupByKey().count(Materialized.as(Stores.inMemoryKeyValueStore("store"))).toStream().to(outputStream2);
        streams = new KafkaStreamsNamedTopologyWrapper(topology1Builder.buildNamedTopology(props), props, clientSupplier);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(singletonList(streams), Duration.ofSeconds(15));

        streams.addNamedTopology(topology2Builder.buildNamedTopology(props));

        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, outputStream1, 3), equalTo(standardOutputData));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, outputStream2, 3), equalTo(standardOutputData));
    }

    @Test
    public void shouldAddNamedTopologyToRunningApplicationWithMulipleNodes() throws Exception {
        produceToInputTopics(inputStream1, standardInputData);
        produceToInputTopics(inputStream2, standardInputData);

        topology1Builder.stream(inputStream1).selectKey((k, v) -> k).groupByKey().count(Materialized.as(Stores.inMemoryKeyValueStore("store"))).toStream().to(outputStream1);
        topology1Builder2.stream(inputStream1).selectKey((k, v) -> k).groupByKey().count(Materialized.as(Stores.inMemoryKeyValueStore("store"))).toStream().to(outputStream1);

        topology2Builder.stream(inputStream2).selectKey((k, v) -> k).groupByKey().count(Materialized.as(Stores.inMemoryKeyValueStore("store"))).toStream().to(outputStream2);
        topology2Builder2.stream(inputStream2).selectKey((k, v) -> k).groupByKey().count(Materialized.as(Stores.inMemoryKeyValueStore("store"))).toStream().to(outputStream2);

        streams = new KafkaStreamsNamedTopologyWrapper(topology1Builder.buildNamedTopology(props), props, clientSupplier);
        streams2 = new KafkaStreamsNamedTopologyWrapper(topology1Builder2.buildNamedTopology(props2), props2, clientSupplier);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(asList(streams, streams2), Duration.ofSeconds(15));

        streams.addNamedTopology(topology2Builder.buildNamedTopology(props));
        streams2.addNamedTopology(topology2Builder2.buildNamedTopology(props2));

        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, outputStream1, 3), equalTo(standardOutputData));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, outputStream2, 3), equalTo(standardOutputData));

        // TODO KAFKA-12648: need to make sure that both instances actually did some of this processing of topology-2,
        //  ie that both joined the group after the new topology was added and then successfully processed records from it
        //  Also: test where we wait for a rebalance between streams.addNamedTopology and streams2.addNamedTopology,
        //  and vice versa, to make sure we hit case where not all new tasks are initially assigned, and when not all yet known
    }

    @Test
    public void shouldRemoveOneNamedTopologyWhileAnotherContinuesProcessing() throws Exception {
        topology1Builder.stream(inputStream1).selectKey((k, v) -> k).groupByKey().count(Materialized.as(Stores.inMemoryKeyValueStore("store"))).toStream().to(outputStream1);
        topology2Builder.stream(inputStream2).selectKey((k, v) -> k).groupByKey().count(Materialized.as(Stores.inMemoryKeyValueStore("store"))).toStream().to(outputStream2);
        streams = new KafkaStreamsNamedTopologyWrapper(buildNamedTopologies(topology1Builder, topology2Builder), props, clientSupplier);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(singletonList(streams), Duration.ofSeconds(15));

        streams.removeNamedTopology("topology-1");

        produceToInputTopics(inputStream1, standardInputData);
        produceToInputTopics(inputStream2, standardInputData);

        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, outputStream2, 3), equalTo(standardOutputData));
    }

    @Test
    public void shouldAddNamedTopologyToRunningApplicationWithMultipleInitialNamedTopologies() throws Exception {
        produceToInputTopics(inputStream1, standardInputData);
        produceToInputTopics(inputStream2, standardInputData);
        produceToInputTopics(inputStream3, standardInputData);

        topology1Builder.stream(inputStream1).selectKey((k, v) -> k).groupByKey().count(Materialized.as(Stores.persistentKeyValueStore("store"))).toStream().to(outputStream1);
        topology2Builder.stream(inputStream2).selectKey((k, v) -> k).groupByKey().count(Materialized.as(Stores.persistentKeyValueStore("store"))).toStream().to(outputStream2);
        topology3Builder.stream(inputStream3).selectKey((k, v) -> k).groupByKey().count(Materialized.as(Stores.persistentKeyValueStore("store"))).toStream().to(outputStream3);
        streams = new KafkaStreamsNamedTopologyWrapper(buildNamedTopologies(topology1Builder, topology2Builder), props, clientSupplier);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(singletonList(streams), Duration.ofSeconds(15));

        streams.addNamedTopology(topology3Builder.buildNamedTopology(props));

        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, outputStream1, 3), equalTo(standardOutputData));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, outputStream2, 3), equalTo(standardOutputData));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, outputStream3, 3), equalTo(standardOutputData));
    }
    
    @Test
    public void shouldAllowPatternSubscriptionWithMultipleNamedTopologies() throws Exception {
        produceToInputTopics(inputStream1, standardInputData);
        produceToInputTopics(inputStream2, standardInputData);
        produceToInputTopics(inputStream3, standardInputData);

        topology1Builder.stream(Pattern.compile(inputStream1)).selectKey((k, v) -> k).groupByKey().count().toStream().to(outputStream1);
        topology2Builder.stream(Pattern.compile(inputStream2)).selectKey((k, v) -> k).groupByKey().count().toStream().to(outputStream2);
        topology3Builder.stream(Pattern.compile(inputStream3)).selectKey((k, v) -> k).groupByKey().count().toStream().to(outputStream3);
        streams = new KafkaStreamsNamedTopologyWrapper(buildNamedTopologies(topology1Builder, topology2Builder, topology3Builder), props, clientSupplier);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(singletonList(streams), Duration.ofSeconds(15));

        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, outputStream1, 3), equalTo(standardOutputData));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, outputStream2, 3), equalTo(standardOutputData));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, outputStream3, 3), equalTo(standardOutputData));
    }

    @Test
    public void shouldAllowMixedCollectionAndPatternSubscriptionWithMultipleNamedTopologies() throws Exception {
        produceToInputTopics(inputStream1, standardInputData);
        produceToInputTopics(inputStream2, standardInputData);
        produceToInputTopics(inputStream3, standardInputData);

        topology1Builder.stream(inputStream1).selectKey((k, v) -> k).groupByKey().count().toStream().to(outputStream1);
        topology2Builder.stream(Pattern.compile(inputStream2)).selectKey((k, v) -> k).groupByKey().count().toStream().to(outputStream2);
        topology3Builder.stream(Pattern.compile(inputStream3)).selectKey((k, v) -> k).groupByKey().count().toStream().to(outputStream3);
        streams = new KafkaStreamsNamedTopologyWrapper(buildNamedTopologies(topology1Builder, topology2Builder, topology3Builder), props, clientSupplier);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(singletonList(streams), Duration.ofSeconds(15));

        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, outputStream1, 3), equalTo(standardOutputData));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, outputStream2, 3), equalTo(standardOutputData));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, outputStream3, 3), equalTo(standardOutputData));
    }

    private void produceToInputTopics(final String topic, final Collection<KeyValue<String, Long>> records) {
        IntegrationTestUtils.produceKeyValuesSynchronously(
            topic,
            records,
            producerConfig,
            CLUSTER.time
        );
    }

    private List<NamedTopology> buildNamedTopologies(final NamedTopologyStreamsBuilder... builders) {
        final List<NamedTopology> topologies = new ArrayList<>();
        for (final NamedTopologyStreamsBuilder builder : builders) {
            topologies.add(builder.buildNamedTopology(props));
        }
        return topologies;
    }
}
