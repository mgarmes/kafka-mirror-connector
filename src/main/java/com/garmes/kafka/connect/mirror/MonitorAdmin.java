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
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MonitorAdmin extends Thread implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(MonitorAdmin.class);

    private boolean preservePartition = true;
    private String TOPIC_RENAME_FORMAT_CONFIG;
    private ConnectorContext context;
    private AdminClient sourceAdminClient;
    private AdminClient targetAdminClient;
    private Set<String> whiteListTopics;
    private Set<String> blackListTopics;
    private Pattern topicPattern;
    
    private long pollIntervalMs;
    private final PartitionAssignor assignor = new RoundRobinAssignor();
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    private volatile List<TopicPartition> currentTopicPartitions = new ArrayList<>();
    private volatile Set<String>  sourceTopics = null;
    private volatile Set<String> targetTopics = null;

    private MonitorAdmin(ConnectorContext context_,
                         String connectorName_,
                         Set<String> blackListTopics_,
                         Set<String> whiteListTopics_,
                         Pattern topicPattern_,
                         int pollIntervalMs_,
                         AdminClient sourceAdminClient_,
                         AdminClient targetAdminClient_,
                         String renameFormat_,
                         Boolean preservePartition_) {
        //rename the thread for best monitoring
        super(connectorName_+"monitor");
        this.context = context_;
        this.whiteListTopics = whiteListTopics_;
        this.topicPattern = topicPattern_;
        this.blackListTopics = blackListTopics_;
        this.pollIntervalMs = pollIntervalMs_;
        this.sourceAdminClient = sourceAdminClient_;
        this.targetAdminClient = targetAdminClient_;
        this.TOPIC_RENAME_FORMAT_CONFIG = renameFormat_;
        this.preservePartition = preservePartition_;
    }

    public MonitorAdmin(ConnectorContext context, MirrorSourceConnectorConfig config) {
        this(context,config.getConnectorName(),
                config.getBlackListTopics(),
                config.getWhiteListTopics(),
                config.getTopicPattern(),
                config.getTopicPollIntervalMs(),
                AdminClient.create(config.sourceAdminClientConfig()),
                AdminClient.create(config.targetAdminClientConfig()),
                config.getTopicRenameFormat(),
                config.getTopicPreservePartitions()
        );
    }


    public synchronized List<List<TopicPartition>> getTasksPartitions(int maxTasks) {

        final long timeout = 30000L;
        long started = System.currentTimeMillis();
        long now = started;
        while (this.currentTopicPartitions.size() ==0 && now - started < timeout) {
            try {
                LOG.info("wait for "+ (timeout - (now - started)));
                wait(timeout - (now - started));
            } catch (InterruptedException e) {
                LOG.error(e.getMessage(),e);
            }
            now = System.currentTimeMillis();
        }

        if (this.sourceTopics == null) {
            throw new ConnectException("Could not obtain topic metadata update from source cluster");
        }
        if (this.targetTopics == null) {
            throw new ConnectException("Could not obtain topic metadata update from destination cluster");
        }
        if (this.sourceTopics.isEmpty()) {
            LOG.info("NO Topic founded in the source cluster");
            return Collections.emptyList();
        }
        if (this.targetTopics.isEmpty()) {
            LOG.info("NO Topic founded in the destination cluster");
            return Collections.emptyList();
        }

        int numTasks = Math.min(this.currentTopicPartitions.size(), maxTasks);
       return groupPartitions(this.currentTopicPartitions, numTasks);
    }

    private List<List<TopicPartition>> groupPartitions(List<TopicPartition> topicPartition, int numTasks){
        List<List<TopicPartition>> roundRobinByTask = new ArrayList<>(numTasks);
        for (int i = 0; i < numTasks; i++) {
            roundRobinByTask.add(new ArrayList<>());
        }
        int count = 0;
        for (TopicPartition partition : topicPartition) {
            int index = count % numTasks;
            roundRobinByTask.get(index).add(partition);
            count++;
        }
        return roundRobinByTask;
    }
    public void start() {
        super.setDaemon(true);
        super.start();
    }

    public void shutdown() {
        this.shutdownLatch.countDown();
        synchronized (this) {
            this.sourceAdminClient.close();
            this.targetAdminClient.close();
        }
    }


    @Override
    public void run() {
        while (shutdownLatch.getCount() > 0) {
            try {
                refreshTopics();
            } catch (Exception e) {
                context.raiseError(e);
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

    private Set<String> topicsToNotReplicate(Set<String> source, Set<String> target){
        Set<String> topics = new HashSet<>();
        source.forEach(x -> {
            if(!target.contains( ConnectHelper.renameTopic(TOPIC_RENAME_FORMAT_CONFIG, x))){
                topics.add(x);
            }
        });
        return topics;
    }

    private List<TopicPartition> topicPartitionsToNotReplicate(List<TopicPartition> source, List<TopicPartition> target){
        List<TopicPartition> topicPartitions = new ArrayList<>();
        source.forEach(x -> {
            if(!target.contains(
                    new TopicPartition(ConnectHelper.renameTopic(TOPIC_RENAME_FORMAT_CONFIG, x.topic()),x.partition())
            )){
                topicPartitions.add(x);
            }
        });
        return topicPartitions;
    }

    private synchronized void refreshTopics() throws InterruptedException, ExecutionException {

        sourceTopics = filterTopics(listTopics(sourceAdminClient));
        targetTopics = listTopics(targetAdminClient);

        // topics marked for replication but doesn't exists in the target cluster
        Set<String> topicsToNotReplicate = topicsToNotReplicate(this.sourceTopics, this.targetTopics);
        if(topicsToNotReplicate.size()!=0){
            LOG.info("Destination topics [{}] don't exists in the target topic, source topics [{}] will not be mirrored",
                    String.join(",", renameTopics(topicsToNotReplicate)),
                    String.join(",", topicsToNotReplicate)
            );
        }

        Set<String> topicToReplicate = new HashSet<>();
        topicToReplicate.addAll(this.sourceTopics);
        topicToReplicate.removeAll(topicsToNotReplicate);


        List<TopicPartition> sourceTopicPartitions = listTopicPartitions(sourceAdminClient, topicToReplicate);
        List<TopicPartition> targetTopicPartitions = listTopicPartitions(targetAdminClient, topicToReplicate);


        List<TopicPartition> topicPartitionsToReplicate = new ArrayList<>();
        topicPartitionsToReplicate.addAll(sourceTopicPartitions);


        if(preservePartition){
            //topicPartitions marked for replication but doesn't exists in the target cluster
            List<TopicPartition>  topicPartitionsToNotReplicate = topicPartitionsToNotReplicate(sourceTopicPartitions, targetTopicPartitions);
            topicPartitionsToReplicate.removeAll(topicPartitionsToNotReplicate);

            if(topicPartitionsToNotReplicate.size()!=0){
                String log_source = topicPartitionsToNotReplicate.stream()
                        .map( Object::toString )
                        .collect( Collectors.joining(","));
                String log_target = topicPartitionsToNotReplicate.stream()
                        .map(x -> new TopicPartition(
                                ConnectHelper.renameTopic(TOPIC_RENAME_FORMAT_CONFIG, x.topic()), x.partition())
                        ).map( Object::toString )
                        .collect( Collectors.joining(","));
                LOG.info("Destination topics [{}] don't exists in the target cluster, source topics [{}] will not be mirrored",
                        log_target, log_source);
            }
        }

        Set<TopicPartition> newTopicPartitions = new HashSet<>();
        newTopicPartitions.addAll(topicPartitionsToReplicate);
        newTopicPartitions.removeAll(this.currentTopicPartitions);
        Set<TopicPartition> deadTopicPartitions = new HashSet<>();
        deadTopicPartitions.addAll(this.currentTopicPartitions);
        deadTopicPartitions.removeAll(topicPartitionsToReplicate);
        if (!newTopicPartitions.isEmpty() || !deadTopicPartitions.isEmpty()) {
            LOG.info("Found {} topic-partitions. {} are new. {} were removed. Previously had {}.",
                    topicPartitionsToReplicate.size(), newTopicPartitions.size(),
                    deadTopicPartitions.size(), currentTopicPartitions.size());

            this.currentTopicPartitions = topicPartitionsToReplicate;
            context.requestTaskReconfiguration();
        }

    }

    // ----------------------
    private boolean matchesTopicPattern(String topic) {
        return (this.topicPattern != null) && (this.topicPattern.matcher(topic).matches());
    }

    public boolean shouldReplicateTopic(String topic) {
        if (ConnectHelper.isInternalTopic(topic)) {
            LOG.info(topic+" is an Internal Topic.");
            return false;
        }
        if (this.blackListTopics.contains(topic)){
            LOG.info(topic+" is back listed.");
            return false;
        }
        if (this.whiteListTopics.contains(topic)){
            LOG.info(topic+" is white listed.");
            return true;
        }
        if (matchesTopicPattern(topic)) {
            LOG.info(topic+" match the regex.");
            return true;
        }
        LOG.info(topic+" does't match any case.");
        return false;
    }

    private static Set<String> listTopics(AdminClient adminClient)
            throws InterruptedException, ExecutionException {
            return adminClient.listTopics().names().get();
    }

    private Collection<TopicDescription> describeTopics(AdminClient adminClient, Collection<String> topics)
            throws InterruptedException, ExecutionException {
            return adminClient.describeTopics(topics).all().get().values();
    }

    private Set<String> filterTopics(Collection<String> topics) {
        return  topics.stream()
                .filter(this::shouldReplicateTopic)
                .collect(Collectors.toSet());
    }

    private Set<String> renameTopics(Collection<String> topics) {
        return  topics.stream()
                .map(x -> ConnectHelper.renameTopic(TOPIC_RENAME_FORMAT_CONFIG, x))
                .collect(Collectors.toSet());
    }

    private Stream<TopicPartition> topicDescriptionToTopicPartition(TopicDescription description) {
        String topic = description.name();
        return description.partitions().stream()
                .map(x -> new TopicPartition(topic, x.partition()));
    }

    private List<TopicPartition> listTopicPartitions(AdminClient adminClient, Collection<String> topics)
            throws InterruptedException, ExecutionException {
        return describeTopics(adminClient, topics).stream()
                .flatMap(this::topicDescriptionToTopicPartition)
                .collect(Collectors.toList());
    }
}

