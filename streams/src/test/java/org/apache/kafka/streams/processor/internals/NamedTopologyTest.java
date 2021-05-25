package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.processor.internals.namedtopology.KafkaStreamsNamedTopologyWrapper;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopology;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopologyStreamsBuilder;
import org.apache.kafka.test.TestUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class NamedTopologyTest {
    final KafkaClientSupplier clientSupplier = new DefaultKafkaClientSupplier();
    final Properties props = configProps();

    final NamedTopologyStreamsBuilder builder1 = new NamedTopologyStreamsBuilder("topology-1");
    final NamedTopologyStreamsBuilder builder2 = new NamedTopologyStreamsBuilder("topology-2");
    final NamedTopologyStreamsBuilder builder3 = new NamedTopologyStreamsBuilder("topology-3");

    KafkaStreamsNamedTopologyWrapper streams;

    @Before
    public void setup() {
        builder1.stream("input-1");
        builder2.stream("input-2");
        builder3.stream("input-3");
    }

    @After
    public void cleanup() {
        if (streams != null) {
            streams.close();
        }
    }

    private static Properties configProps() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Named-Topology-App");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2018");
        props.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        return props;
    }

    @Test
    public void shouldThrowIllegalArgumentOnIllegalName() {
        assertThrows(IllegalArgumentException.class, () -> new NamedTopologyStreamsBuilder("**-not-allowed-**"));
    }

    @Test
    public void shouldStartUpEmptyNamedTopology() throws Exception {
        streams = new KafkaStreamsNamedTopologyWrapper(props, clientSupplier);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(singletonList(streams), Duration.ofSeconds(15));
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

    @Test
    public void shouldReturnTopologyByName() {
        final NamedTopology topology1 = builder1.buildNamedTopology(props);
        final NamedTopology topology2 = builder2.buildNamedTopology(props);
        final NamedTopology topology3 = builder3.buildNamedTopology(props);
        streams = new KafkaStreamsNamedTopologyWrapper(asList(topology1, topology2, topology3), props, clientSupplier);
        assertThat(streams.getTopologyByName("topology-1"), equalTo(topology1));
        assertThat(streams.getTopologyByName("topology-2"), equalTo(topology2));
        assertThat(streams.getTopologyByName("topology-3"), equalTo(topology3));
    }

    @Test
    public void shouldThrowIllegalArgumentWhenLookingUpNonExistentTopologyByName() {
        streams = new KafkaStreamsNamedTopologyWrapper(buildNamedTopologies(builder1), props, clientSupplier);
        assertThrows(IllegalArgumentException.class, () -> streams.getTopologyByName("non-existent-topology"));
    }

    @Test
    public void shouldDescribeWithSingleNamedTopology() {
        builder1.stream("input").filter((k, v) -> !k.equals(v)).to("output");
        throw new AssertionError("TODO KAFKA-12648");
    }

    @Test
    public void shouldDescribeWithMultipleNamedTopologies() {
        throw new AssertionError("TODO KAFKA-12648");
    }

    @Test
    public void shouldDescribeWithEmptyNamedTopology() {
        throw new AssertionError("TODO KAFKA-12648");
    }

    private List<NamedTopology> buildNamedTopologies(final NamedTopologyStreamsBuilder... builders) {
        final List<NamedTopology> topologies = new ArrayList<>();
        for (final NamedTopologyStreamsBuilder builder : builders) {
            topologies.add(builder.buildNamedTopology(props));
        }
        return topologies;
    }
}
