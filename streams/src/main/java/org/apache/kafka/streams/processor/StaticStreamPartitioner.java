package org.apache.kafka.streams.processor;

import org.apache.kafka.streams.errors.TaskMigratedException;

/**
 * A special implementation of the {@link StreamPartitioner} interface which enforces that keys are "statically
 * partitioned" ie sent to the same partition regardless of changes to the underlying topic partition count.
 * Implementing this and passing it in to the {@code static.partitioner.class} config will enable automatic
 * partition scaling for the Streams application.
 *
 * Automatic partition scaling for Streams applications with static partitioning means that Streams will detect
 * any changes (increases) to the external user topics' partition counts, and automatically adjust the partitions
 * of any internal topics managed by Streams.
 *
 * Besides the static partitioning constraint, to best utilize this feature we recommend that users perform any
 * partition expansions starting with the most downstream output topics, and working upstream from there to the
 * input topics. This will help Streams to stabilize with the least amount of disruption and/or downtime.
 *
 * It is also recommended to avoid sending any records with keys that are to go into the new partitions until after
 * Streams has fully processed the expansion. The {@link #onPartitionExpansion(int, int)} method can be implemented
 * as a callback to be notified when Streams has finished expanding any internal topics.
 */
public interface StaticStreamPartitioner<K, V> extends StreamPartitioner<K, V> {

    /**
     * Determine the partition number for a record with the given key and the current number of partitions.
     * The partition number must be static and deterministic by key, meaning that any and all future
     * records with the same key will always be placed into the same partition, regardless of a change
     * in the number of partitions. In other words, should the number of partitions increase, only new keys
     * may be routed to the new partitions; all keys that have been seen before should end up in their original
     * partitions. It is up to the user to enforce this.
     *
     * If the partitioning is not static, autoscaling of internal topics may cause incorrect results if keys
     * end up getting routed to different partitions when the {@code numPartitions} changes.
     *
     * @param topic the topic name this record is sent to
     * @param key the key of the record
     * @param value the value of the record
     * @param numPartitions the total number of partitions
     * @return an integer between 0 and {@code numPartitions - 1}
     *
     * @throws TaskMigratedException if the partitioner requires more partitions/wants to send the given key to
     *                               a partition that doesn't exist yet/is higher than the upper limit of
     *                               {@code numPartitions-1}. This should only happen when the user failed to wait
     *                               for Streams to scale out/restabilize after a partition expansion, thus throwing
     *                               this exception tells Streams to rebalance and recheck the number of partitions
     *                               throughout the topology.
     */
    int staticPartition(String topic, K key, V value, int numPartitions);

    /**
     * An optional callback for the user to take some action when Streams has detected and finished reacting to
     * a change in the number of partitions. This callback will be invoked once the application has re-stabilized
     * to a consistent number of partitions across the topology.
     *
     * Note: this will only be triggered once per rebalance/partition expansion, for the member which is the group
     * leader for that rebalance. In other words, expect only a single StreamThreads in a single application instance
     * to invoke this callback.
     *
     * @param oldNumPartitions
     * @param newNumPartitions
     */
    default void onPartitionExpansion(int oldNumPartitions, int newNumPartitions) { }

    @Override
    default Integer partition(String topic, K key, V value, int numPartitions) {
        return staticPartition(topic, key, value, numPartitions);
    }
}
