/*
 * kafka-connect-mirror - Apache Kafka connector to mirror data
 *
 * Copyright (c) 2018, Mohammed Amine GARMES
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

package com.garmes.kafka.connect.mirror;

import com.garmes.kafka.connect.mirror.utils.ConnectHelper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class Monitor extends Thread implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(Monitor.class);

    private boolean preservePartition = true;
    private String TOPIC_RENAME_FORMAT_CONFIG;
    private ConnectorContext context;
    private Consumer<byte[], byte[]> src_consumer;
    private Consumer<byte[], byte[]> dest_consumer;
    private Set<String> whiteListTopics;
    private Pattern topicPattern;
    private Set<String> blackListTopics;
    private long pollIntervalMs;
    private final PartitionAssignor assignor = new RoundRobinAssignor();
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    private volatile Map<String, List<PartitionInfo>> src_cluster_topics = null;
    private volatile Map<String, List<PartitionInfo>> dest_cluster_topics = null;

    private Monitor(ConnectorContext context_,
                    String connectorName_,
                    Set<String> blackListTopics_,
                    Set<String> whiteListTopics_,
                    Pattern topicPattern_,
                    int pollIntervalMs_,
                    Consumer<byte[], byte[]> src_consumer_crawler_,
                    Consumer<byte[], byte[]> dest_consumer_crawler_,
                    String rename_format_,
                    Boolean preservePartition_) {
        //rename the thread for best monitoring
        super(connectorName_+"monitor");
        this.context = context_;
        this.whiteListTopics = whiteListTopics_;
        this.topicPattern = topicPattern_;
        this.blackListTopics = blackListTopics_;
        this.pollIntervalMs = pollIntervalMs_;
        this.src_consumer = src_consumer_crawler_;
        this.dest_consumer = dest_consumer_crawler_;
        this.TOPIC_RENAME_FORMAT_CONFIG = rename_format_;
        this.preservePartition = preservePartition_;
    }

    public Monitor(ConnectorContext context, MirrorSourceConnectorConfig config) {
        this(context,config.getConnectorName(),
                config.getBlackListTopics(),
                config.getWhiteListTopics(),
                config.getTopicPattern(),
                config.getTopicPollIntervalMs(),
                buildSrcConsumerCrawler(config),
                buildDestConsumerCrawler(config),
                config.getTopicRenameFormat(),
                config.getTopicPreservePartitions()
        );
    }


    public synchronized Map<String, PartitionAssignor.Assignment> getTasksPartitions(int maxTasks) {
        final long timeout = 30000L;
        long started = System.currentTimeMillis();
        long now = started;
        while (this.src_cluster_topics == null && now - started < timeout) {
            try {
                LOG.info("wait fro "+ (timeout - (now - started)));
                wait(timeout - (now - started));
            } catch (InterruptedException e) {
                LOG.error(e.getMessage(),e);
            }
            now = System.currentTimeMillis();
        }

        if (this.src_cluster_topics == null) {
            throw new ConnectException("Could not obtain topic metadata update from source cluster");
        }
        if (this.dest_cluster_topics == null) {
            throw new ConnectException("Could not obtain topic metadata update from destination cluster");
        }
        if (this.src_cluster_topics.isEmpty()) {
            LOG.info("NO Topic founded in the source cluster");
            return Collections.emptyMap();
        }
        if (this.dest_cluster_topics.isEmpty()) {
            LOG.info("NO Topic founded in the destination cluster");
            return Collections.emptyMap();
        }

        int numTasks = Math.min(numPartitionsToAssign(this.src_cluster_topics), maxTasks);

        //use the consumer assignor instead of 'ConnectorUtils.groupPartitions'
        List<String> topics = new ArrayList<String>(this.src_cluster_topics.keySet());
        Cluster cluster = clusterMetadata(this.src_cluster_topics);
        Map<String, PartitionAssignor.Subscription> subscriptions = new HashMap<>(numTasks);
        for (int i = 0; i < numTasks; i++) {
            subscriptions.put(String.format("task-%d", i), new PartitionAssignor.Subscription(topics));
        }
        // find the example at org.apache.kafka.common.TopicPartition.RoundRobinAssignorTest
       return this.assignor.assign(cluster, subscriptions);
    }

    public void start() {
        super.setDaemon(true);
        super.start();
    }

    public void shutdown() {
        this.src_consumer.wakeup();
        this.dest_consumer.wakeup();

        this.shutdownLatch.countDown();
        synchronized (this) {
            this.src_consumer.close();
            this.dest_consumer.close();
        }
    }



    @Override
    public void run() {
        while (shutdownLatch.getCount() > 0) {
            try {
                if (updateTopics()) {
                    context.requestTaskReconfiguration();
                }
            } catch (Exception e) {
                context.raiseError(e);
                throw e;
            }

            try {
                boolean shuttingDown = shutdownLatch.await(pollIntervalMs, TimeUnit.MILLISECONDS);
                if (shuttingDown) {
                    return;
                }
            } catch (InterruptedException e) {
                LOG.error("Unexpected InterruptedException, ignoring: ", e);
            }
        }

    }

    private synchronized boolean updateTopics() {
        Map<String, List<PartitionInfo>> updatedTopics = listMatchingTopics();
        this.dest_cluster_topics = getDestTopics();

        if (LOG.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder();
            sb.append("Topic on Dest Cluster: ");
            for (String topic : this.dest_cluster_topics.keySet()) {
                sb.append(String.format("%s, ", topic));
            }
            LOG.info(sb.toString());
        }
        removeNonExistingPartitions(updatedTopics, this.dest_cluster_topics);
        Map<String, List<PartitionInfo>> currentTopics = this.src_cluster_topics;
        this.src_cluster_topics = updatedTopics;

        if (!matchesPartitions(currentTopics, updatedTopics)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("After filtering we got Topics|Partitions: " + ConnectHelper.topicMetadataToString(currentTopics));
            }
            notifyAll();
            return currentTopics != null;
        }
        return false;
    }

    private boolean matchesPartitions(Map<String, List<PartitionInfo>> topicMetadataA,
                                      Map<String, List<PartitionInfo>> topicMetadataB) {
        if(topicMetadataA == null ) return false;
        if (topicMetadataA.size() != topicMetadataB.size()) {
            return false;
        }
        for (Map.Entry<String, List<PartitionInfo>> topicEntry : topicMetadataA.entrySet()) {
            List<PartitionInfo> oldMetadata = topicMetadataB.get(topicEntry.getKey());
            List<PartitionInfo> newMetadata = topicEntry.getValue();
            if ((oldMetadata == null) || (newMetadata.size() != oldMetadata.size())) {
                return false;
            }
        }
        return true;
    }
    private Boolean partitionExists(int partition, List<PartitionInfo> topicPartitionsInfo) {
        for (PartitionInfo topicPartitionInfo : topicPartitionsInfo) {
            if (topicPartitionInfo.partition() == partition) return true;
        }
        return false;
    }

    private void removeNonExistingPartitions(Map<String, List<PartitionInfo>> src_topicPartitions,
                                             Map<String, List<PartitionInfo>> dest_topicPartitions) {

        String[] src_topics = new String[src_topicPartitions.size()];
        src_topicPartitions.keySet().toArray(src_topics);

        for (Map.Entry<String, List<PartitionInfo>> entry : src_topicPartitions.entrySet()) {
            String src_topic = entry.getKey();
            String dest_topic = ConnectHelper.renameTopic(TOPIC_RENAME_FORMAT_CONFIG, src_topic);
            List<PartitionInfo> src_topic_partitions = entry.getValue();
            List<PartitionInfo> dest_topic_partitions = dest_topicPartitions.getOrDefault(dest_topic, Collections.emptyList());
            if (dest_topic_partitions.size() == 0) {
                LOG.warn("Destination topic don't exists {}, source topic {} will not be mirrored", dest_topic, src_topic);
                src_topicPartitions.remove(src_topic);
                continue;
            }
            if (!preservePartition) {
                return;
            }
            if (src_topic_partitions.size() < dest_topic_partitions.size()) {
                LOG.warn("Destination topic '{}' have more partitions than source topic '{}' .", dest_topic, src_topic);
            } else if (src_topic_partitions.size() > dest_topic_partitions.size()) {

                LOG.warn("Source topic '{}' have more partitions than destination topic '{}', some partition will be not mirrored .", src_topic, dest_topic);

                List<PartitionInfo> to = new ArrayList<>();
                for (Iterator<PartitionInfo> i = src_topic_partitions.iterator(); i.hasNext(); ) {
                    PartitionInfo element = i.next();
                    if (partitionExists(element.partition(), dest_topic_partitions)) {

                        to.add(element);
                    } else {
                        LOG.warn("Partition Will be not mirrored {} {}.", element.topic(), element.partition());
                    }
                }
                entry.setValue(to);
            }
        }
    }

    private int numPartitionsToAssign(Map<String, List<PartitionInfo>> topicsMetadata) {
        int numPartitions = 0;
        for (List<PartitionInfo> partitions : topicsMetadata.values()) {
            numPartitions += partitions.size();
        }
        return numPartitions;
    }

    // cluster metadata will be used only with the assignor
    private Cluster clusterMetadata(Map<String, List<PartitionInfo>> srcTopics) {
        Set<Node> nodes = new HashSet<>();
        List<PartitionInfo> partitionInfos = new ArrayList<>();
        for (Map.Entry<String, List<PartitionInfo>> topicEntry : srcTopics.entrySet()) {
            partitionInfos.addAll(topicEntry.getValue());
            for (PartitionInfo partitionInfo : topicEntry.getValue()) {
                Collections.addAll(nodes, partitionInfo.replicas());
            }
        }
        return new Cluster(null, nodes, partitionInfos,
                Collections.emptySet(),
                Collections.emptySet());
    }

    private boolean matchesTopicPattern(String topic) {
        return (this.topicPattern != null) && (this.topicPattern.matcher(topic).matches());
    }

    private Map<String, List<PartitionInfo>> listMatchingTopics() {
        Map<String, List<PartitionInfo>> topicMetadata = this.src_consumer.listTopics();
        LOG.info("src topicMetadata size: "+topicMetadata.size());
        Map<String, List<PartitionInfo>> matchingTopics = new HashMap<>();
        for (Map.Entry<String, List<PartitionInfo>> topicEntry : topicMetadata.entrySet()) {
            String topic = topicEntry.getKey();

            if (ConnectHelper.isInternalTopic(topic)) {
                LOG.info(topic+" is an Internal Topic.");
                continue;
            }

            if (this.blackListTopics.contains(topic)){
                LOG.info(topic+" is back listed.");
                continue;
            }

            if (this.whiteListTopics.contains(topic)){
                LOG.info(topic+" is white listed.");
                matchingTopics.put(topic, topicEntry.getValue());
                continue;
            }

            if (matchesTopicPattern(topic)) {
                LOG.info(topic+" match the regex.");
                matchingTopics.put(topic, topicEntry.getValue());
                continue;
            }

            LOG.info(topic+" does't match any case.");

        }
        LOG.info("src topic matching size: "+matchingTopics.size());
        return matchingTopics;
    }

    private Map<String, List<PartitionInfo>> getDestTopics() {
        return this.dest_consumer.listTopics();
    }

    // used to get Topic Partitions metadata from source cluster
    // TODO use kafka admin client
    private static KafkaConsumer<byte[], byte[]> buildSrcConsumerCrawler(MirrorSourceConnectorConfig config) {
        Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.putAll(config.getSrcConsumerConfigs());
        if (!consumerConfig.containsKey("client.id")) {
            consumerConfig.put("client.id", config.getConnectorName()+"-kafka-src-monitor");
        }
        return new KafkaConsumer<>(consumerConfig, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    // used to get Topic Partitions metadata from dest cluster
    // TODO use kafka admin client
    private static KafkaConsumer<byte[], byte[]> buildDestConsumerCrawler(MirrorSourceConnectorConfig config) {
        Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.putAll(config.getDestConsumerConfigs());
        if (!consumerConfig.containsKey("client.id")) {
            consumerConfig.put("client.id",config.getConnectorName()+"-kafka-dest-monitor");
        }
        return new KafkaConsumer<>(consumerConfig, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }
}

