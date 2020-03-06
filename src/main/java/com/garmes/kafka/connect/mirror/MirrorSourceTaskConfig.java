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
import com.garmes.kafka.connect.mirror.utils.MirrorMetrics;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MirrorSourceTaskConfig extends MirrorSourceConnectorConfig {

    private static final Logger LOG = LoggerFactory.getLogger(MirrorSourceTaskConfig.class);

    public static final String PARTITION_CONFIG = "task.partitions";
    private static final String PARTITION_DOC = "List of Partition to be mirrored.";
    public static final String ID_CONFIG = "task.id";
    private static final String ID_DOC = "task id";

    private static final ConfigDef config = baseConfigDef()
            .define(PARTITION_CONFIG,
                    ConfigDef.Type.LIST,
                    "",
                    ConfigDef.Importance.HIGH,
                    PARTITION_DOC)
            .define(ID_CONFIG,
                    ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH,
                    ID_DOC);

    public MirrorSourceTaskConfig(Map<String, String> properties) {
        super(config, properties);
    }

    public List<TopicPartition> getPartitions() {

        List<String> fields = getList(PARTITION_CONFIG);
        if (fields == null || fields.isEmpty()) {
            return Collections.emptyList();
        }

        LOG.info(String.join(",", fields));

        return fields.stream()
                .map(ConnectHelper::decodeTopicPartition)
                .collect(Collectors.toList());

    }

    public String getId() {
        return getString(ID_CONFIG);
    }


    public static MirrorSourceTaskConfig create(MirrorSourceConnectorConfig config,
                                                 List<TopicPartition> topicPartitions,
                                                 String id) {
        Map configCopy = new HashMap(config.originalsStrings());
        configCopy.put(PARTITION_CONFIG, ConnectHelper.encodeTaskPartitions(topicPartitions));
        configCopy.put(ID_CONFIG, id);
        return new MirrorSourceTaskConfig(configCopy);
    }

    MirrorMetrics metrics() {
        MirrorMetrics metrics = new MirrorMetrics(this);
        metricsReporters().forEach(metrics::addReporter);
        return metrics;
    }


}
