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

import com.garmes.kafka.connect.mirror.utils.ByteArrayConverter;
import com.garmes.kafka.connect.mirror.utils.ConnectHelper;
import com.garmes.kafka.connect.mirror.utils.Version;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class MirrorSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(MirrorSourceTask.class);
    private MirrorSourceTaskConfig config;
    private Converter sourceKeyConverter;
    private Converter sourceValueConverter;
    private volatile Consumer<byte[], byte[]> consumer;

    public MirrorSourceTask() {
    }

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
        try {
            this.config = new MirrorSourceTaskConfig(properties);
        } catch (ConfigException e) {
            throw new ConnectException("Failed to start Kafka mirror task due to configuration error", e);
        }

        this.sourceKeyConverter = new ByteArrayConverter();
        this.sourceValueConverter = new ByteArrayConverter();
        this.consumer = buildConsumer(this.config);

        Collection<TopicPartition> partitions = this.config.getPartitions().partitions();
        initConsumer(partitions);
        log.info("Started kafka mirror task, mirroring partitions {}", partitions);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        try {

            ConsumerRecords<byte[], byte[]> records = this.consumer.poll(100);
            if (records.isEmpty()) {
                return Collections.emptyList();
            }
            boolean preservePartitions = this.config.getTopicPreservePartitions();
            List<SourceRecord> sourceRecords = new ArrayList<>(records.count());
            for (Iterator i$ = records.partitions().iterator(); i$.hasNext(); ) {
                TopicPartition topicPartition = (TopicPartition) i$.next();
                String sourceTopic = topicPartition.topic();
                String destTopic = toDestTopic(sourceTopic);

                int partition = topicPartition.partition();
                Map<String, ?> connectSourcePartition = ConnectHelper.toConnectPartition(sourceTopic, partition);
                for (ConsumerRecord<byte[], byte[]> record : records.records(topicPartition)) {
                    Map<String, Object> connectOffset = ConnectHelper.toConnectOffset(record.offset());
                    SchemaAndValue key = this.sourceKeyConverter.toConnectData(sourceTopic, record.key());
                    SchemaAndValue value = this.sourceValueConverter.toConnectData(sourceTopic, record.value());
                    Long timestamp;
                    if (record.timestamp() >= 0L) {
                        timestamp = record.timestamp();
                    } else {
                        if (record.timestamp() == -1L) {
                            timestamp = null;
                        } else {
                            throw new CorruptRecordException(String.format("Invalid Record timestamp: %d", record.timestamp()));
                        }
                    }

                    Integer destPartition = preservePartitions ? partition : null;

                    sourceRecords.add(new SourceRecord(connectSourcePartition,
                                    connectOffset,
                                    destTopic,
                                    destPartition,
                                    key.schema(),
                                    key.value(),
                                    value.schema(),
                                    value.value(),
                                    timestamp));
                }
            }

            return sourceRecords;
        } catch (OffsetOutOfRangeException e) {
            Map<TopicPartition, Long> outOfRangePartitions = e.offsetOutOfRangePartitions();
            log.warn("Consumer from source cluster detected out of range partitions: {}", outOfRangePartitions);

            this.consumer.seekToBeginning(outOfRangePartitions.keySet());
            return Collections.emptyList();
        } catch (WakeupException e) {
            log.debug("Kafka mirror task woken up");
        }
        return Collections.emptyList();
    }

    @Override
    public void stop() {
        log.info("Closing kafka mirror task");
        if (this.consumer != null) {
            this.consumer.wakeup();
            synchronized (this) {
                ClientUtils.closeQuietly(this.consumer, "consumer", new AtomicReference());
            }
        }
    }


    synchronized void initConsumer(Collection<TopicPartition> partitions) {

        this.consumer.assign(partitions);

        for (TopicPartition partition : partitions) {
            Map<String, ?> connectSourcePartition = ConnectHelper.toConnectPartition(partition);
            // Get the latest committed offset
            Map<String, Object> connectSourceOffset = this.context.offsetStorageReader().offset(connectSourcePartition);
            if (connectSourceOffset == null) {
                log.debug("No offset Founded for {}, seek to the beginning ", partition);
                this.consumer.seekToBeginning(Collections.singleton(partition));
            } else {
                long offset = (Long) connectSourceOffset.get("offset");
                log.debug("Use committed offset {} for partition {}", offset, partition);
                this.consumer.seek(partition, offset + 1L);
            }
        }
    }

    private Consumer<byte[], byte[]> buildConsumer(MirrorSourceTaskConfig config) {
        if (this.consumer != null) {
            return this.consumer;
        }
        Map<String, Object> consumerConfig = new HashMap();
        consumerConfig.putAll(config.getSrcConsumerConfigs());

        /*
        we can not run meany consumer in the same JVM with the same client.id  {bean conflict}
        javax.management.InstanceAlreadyExistsException: kafka.consumer:type=app-info,id=kafka-mirror-client
         if (!consumerConfig.containsKey("client.id")) {
            consumerConfig.put("client.id", "kafka-mirror-client");
        }

        Random randomGenerator = new Random();
        consumerConfig.put("client.id", "kafka-mirror-client"+randomGenerator.nextInt(1000));
         */

        consumerConfig.put("client.id", this.config.getId());
        consumerConfig.put("enable.auto.commit", false);
        consumerConfig.put("auto.offset.reset", "none");
        return new KafkaConsumer(consumerConfig, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }


    // Helpers -------------------
    private String toDestTopic(String sourceTopic) {
        return ConnectHelper.renameTopic(this.config.getTopicRenameFormat(), sourceTopic);
    }

}
