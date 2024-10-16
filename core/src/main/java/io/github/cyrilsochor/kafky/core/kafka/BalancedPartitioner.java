package io.github.cyrilsochor.kafky.core.kafka;

import io.github.cyrilsochor.kafky.core.pair.PairMatcher;
import io.github.cyrilsochor.kafky.core.util.FixedSizeMap;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The balanced partitioner uses "Round-Robin" algorithm using all partitions of
 * the topic (standard RoundRobinPartitioner uses only available partitions).
 * 
 * This partitioning strategy can be used when user wants to distribute the
 * writes to all partitions equally. This is the behaviour regardless of record
 * key hash.
 *
 * @see RoundRobinPartitioner
 */
public class BalancedPartitioner implements Partitioner {

    private static final Logger LOG = LoggerFactory.getLogger(PairMatcher.class);

    protected final ConcurrentMap<String, AtomicInteger> topicCounterMap = new ConcurrentHashMap<>();
    protected final Map<Integer, Integer> partitionCache = new FixedSizeMap<>(100);

    @Override
    public void configure(Map<String, ?> configs) {
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
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // this method is called by kafka multiple times with same parameters -> the result must be cached in partitionCache

        final int partition;
        if (valueBytes == null) {
            partition = computeNext(topic, cluster);
        } else {
            partition = partitionCache.computeIfAbsent(System.identityHashCode(valueBytes), t -> computeNext(topic, cluster));
        }

        LOG.info("Computed partition: {}", partition);
        return partition;
    }

    @Override
    public void close() {
    }

    protected int computeNext(String topic, Cluster cluster) {
        final List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        return Utils.toPositive(topicCounterMap.computeIfAbsent(topic, i -> new AtomicInteger(0))
                .getAndIncrement())
                % partitions.size();
    }

}
