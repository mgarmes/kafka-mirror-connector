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

import com.garmes.kafka.connect.mirror.utils.Version;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class MirrorSourceConnector extends SourceConnector {
    private static final Logger LOG = LoggerFactory.getLogger(MirrorSourceConnector.class);
    private MonitorAdmin monitor;
    private MirrorSourceConnectorConfig config;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> configProperties) {

        try {
            config = new MirrorSourceConnectorConfig(configProperties);
        } catch (ConfigException e) {
            throw new ConnectException("Couldn't start MirrorSourceConnector due to configuration error", e);
        }

        LOG.info("Starting mirror connector");

        this.monitor = new MonitorAdmin(this.context, this.config);
        this.monitor.start();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MirrorSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {

        List<Map<String, String>> taskConfigs = new ArrayList<>();

        List<List<TopicPartition>> tasksPartitions = this.monitor.getTasksPartitions(maxTasks);
        if (tasksPartitions.isEmpty()) {
            return Collections.emptyList();
        }

        for(int i=0; i<tasksPartitions.size();i++){
            taskConfigs.add(MirrorSourceTaskConfig.create(
                    this.config,
                    tasksPartitions.get(i),
                    Integer.toString(i)).originalsStrings()
            );
        }

        return taskConfigs;
    }

    @Override
    public void stop() {
        LOG.info("Shutting down mirror connector.");
        if (this.monitor != null) {
            this.monitor.shutdown();
        }
    }

    @Override
    public ConfigDef config() {
        return MirrorSourceConnectorConfig.CONFIG_DEF;
    }

}
