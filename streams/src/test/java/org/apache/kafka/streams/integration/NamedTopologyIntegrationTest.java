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
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.apache.kafka.streams.processor.internals.namedtopology.KafkaStreamsNamedTopologyWrapper;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopology;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopologyStreamsBuilder;
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
import java.util.List;
import java.util.Properties;

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;

import static java.util.Collections.singletonList;

public class NamedTopologyIntegrationTest {
    // TODO KAFKA-12648
    /**
     * Things to test in Pt. 2 -  Introduce TopologyMetadata to wrap InternalTopologyBuilders of named topologies:
     * 1. Verify changelog & repartition topics decorated with named topology
     * 2. Make sure a complex app run and works, ie one with
     *         -multiple subtopologies
     *         -persistent state
     *         -multi-partition input & output topics
     *         -standbys
     *         -piped input and verified output records
     * 3. Is the task assignment balanced? Does KIP-441/warmup replica placement work as intended?
     *
     * Things to test in Pt. 3 - implement addNamedTopology() API for additive upgrades:
     * 1. Test starting up Streams with empty topology
     * 2. Test starting up Streams with multiple NamedTopologies (not dynamically added)
     * 3. Verify complex app (see Pt.2-2 above) works when a second NamedTopology is added after startup, ie call addNamedTopology()
     *
     */

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
    private String inputStream;

    final KafkaClientSupplier clientSupplier = new DefaultKafkaClientSupplier();

    final NamedTopologyStreamsBuilder builder1 = new NamedTopologyStreamsBuilder("topology-1");
    final NamedTopologyStreamsBuilder builder2 = new NamedTopologyStreamsBuilder("topology-2");
    final NamedTopologyStreamsBuilder builder3 = new NamedTopologyStreamsBuilder("topology-3");

    Properties props;
    KafkaStreamsNamedTopologyWrapper streams;

    private Properties configProps() {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(appId).getPath());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000L);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return streamsConfiguration;
    }

    @Before
    public void setup() throws InterruptedException {
        appId = safeUniqueTestName(NamedTopologyIntegrationTest.class, testName);
        inputStream = appId + "-input-stream";
        props = configProps();
        CLUSTER.createTopic(inputStream, 2, 1);
    }

    @After
    public void shutdown() {
        if (streams != null) {
            streams.close(Duration.ofSeconds(30));
        }
    }

    @Test
    public void shouldStartUpSingleNamedTopology() throws Exception {
        streams = new KafkaStreamsNamedTopologyWrapper(builder1.buildNamedTopology(props), props, clientSupplier);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(singletonList(streams), Duration.ofSeconds(15));
    }

    @Test
    public void shouldStartUpMultipleNamedTopologies() throws Exception {
        streams = new KafkaStreamsNamedTopologyWrapper(buildNamedTopologies(builder1, builder2, builder3), props, clientSupplier);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(singletonList(streams), Duration.ofSeconds(15));
    }

    private List<NamedTopology> buildNamedTopologies(final NamedTopologyStreamsBuilder... builders) {
        final List<NamedTopology> topologies = new ArrayList<>();
        for (final NamedTopologyStreamsBuilder builder : builders) {
            topologies.add(builder.buildNamedTopology(props));
        }
        return topologies;
    }

    private void buildComplexTopology(final NamedTopologyStreamsBuilder builder) {

    }
}
