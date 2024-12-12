package io.github.cyrilsochor.kafky.core.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The balanced partitioner uses "Round-Robin" algorithm using all partitions of
 * the topic. MODIFIED TO WORK PROPERLY WITH STICKY PARTITIONING (KIP-480) -
 * USING <a href="https://github.com/apache/kafka/pull/11326">FIX</a>.
 * 
 * This partitioning strategy can be used when user wants to distribute the
 * writes to all partitions equally. This is the behaviour regardless of record
 * key hash.
 *
 * @see DefaultPartitioner
 * @see RoundRobinPartitioner
 */
public class BalancedPartitioner implements Partitioner {

    private static final Logger LOG = LoggerFactory.getLogger(BalancedPartitioner.class);

    protected final ConcurrentMap<String, AtomicInteger> topicCounterMap = new ConcurrentHashMap<>();
    protected final ConcurrentMap<String, Queue<Integer>> topicPartitionQueueMap = new ConcurrentHashMap<>();

    @Override
    public void configure(Map<String, ?> configs) {
        // noting to do
    }

    /**
     * Compute the partition for the given record.
     *
     * @param topic
     *            The topic name
     * @param key
     *            The key to partition on (or null if no key)
     * @param keyBytes
     *            serialized key to partition on (or null if no key)
     * @param value
     *            The value to partition on or null
     * @param valueBytes
     *            serialized value to partition on or null
     * @param cluster
     *            The current cluster metadata
     */
    @Override
    public int partition(
            String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        final Queue<Integer> partitionQueue = partitionQueueComputeIfAbsent(topic);
        final Integer queuedPartition = partitionQueue.poll();
        if (queuedPartition != null) {
            LOG.trace("Partition chosen from queue: {}", queuedPartition);
            return queuedPartition;
        } else {
            final List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
            final int numPartitions = partitions.size();
            final int nextValue = nextValue(topic);
            final List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);
            if (!availablePartitions.isEmpty()) {
                int part = Utils.toPositive(nextValue) % availablePartitions.size();
                int partition = availablePartitions.get(part).partition();
                LOG.trace("Partition chosen: {}", partition);
                return partition;
            } else {
                // no partitions are available, give a non-available partition
                return Utils.toPositive(nextValue) % numPartitions;
            }
        }
    }

    protected int nextValue(String topic) {
        final AtomicInteger counter = topicCounterMap.computeIfAbsent(
                topic,
                k -> new AtomicInteger(0));
        return counter.getAndIncrement();
    }

    protected Queue<Integer> partitionQueueComputeIfAbsent(String topic) {
        return topicPartitionQueueMap.computeIfAbsent(topic, k -> new ConcurrentLinkedQueue<>());
    }

    @Override
    public void close() {
        // noting to do
    }

    /**
     * Notifies the partitioner a new batch is about to be created. When using
     * the sticky partitioner, this method can change the chosen sticky
     * partition for the new batch.
     *
     * @param topic
     *            The topic name
     * @param cluster
     *            The current cluster metadata
     * @param prevPartition
     *            The partition previously selected for the record that
     *            triggered a new batch
     * @deprecated
     */
    @Override
    @Deprecated(forRemoval = true, since = "kafka-3.3.0")
    @SuppressWarnings("java:S1133")
    public void onNewBatch(String topic, Cluster cluster, int prevPartition) {
        LOG.trace("New batch so enqueuing partition {} for topic {}", prevPartition, topic);
        final Queue<Integer> partitionQueue = partitionQueueComputeIfAbsent(topic);
        partitionQueue.add(prevPartition);
    }

}
