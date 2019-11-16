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


import com.garmes.kafka.connect.mirror.utils.RegexValidator;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricsReporter;

import java.util.*;
import java.util.regex.Pattern;

public class MirrorSourceConnectorConfig extends AbstractConfig {

    MirrorSourceConnectorConfig(ConfigDef subclassConfigDef, Map<String, String> props) {

        super(subclassConfigDef, props);
    }

    MirrorSourceConnectorConfig(Map<String, String> props) {
        super(CONFIG_DEF, props);
    }

    static final ConfigDef CONFIG_DEF = baseConfigDef();

    static ConfigDef baseConfigDef() {


        ConfigDef config = new ConfigDef();
        addConnectorOptions(config);
        return config;
    }

    private static final String CONNECTOR_NAME_CONFIG = "name";
    private static final String CONNECTOR_NAME_DOC = "Unique connector name";
    private static final String CONNECTOR_NAME_DISPLAY = "connector name";

    private static final String TOPIC_WHITELIST_CONFIG = "topic.whitelist";
    private static final String TOPIC_WHITELIST_DOC = "Whitelist of topics to be mirrored.";
    //public static final List TOPIC_WHITELIST_DEFAULT = Collections.emptyList();
    private static final String TOPIC_WHITELIST_DISPLAY = "topic whitelist";

    private static final String TOPIC_BLACKLIST_CONFIG = "topic.blacklist";
    private static final String TOPIC_BLACKLIST_DOC = "Topics to exclude from mirroring.";
    //public static final List TOPIC_BLACKLIST_DEFAULT = Collections.emptyList();
    private static final String TOPIC_BLACKLIST_DISPLAY = "topic blacklist";

    private static final String TOPIC_REGEX_CONFIG = "topic.regex";
    private static final String TOPIC_REGEX_DOC = "Regex of topics to mirror.";
    private static final String TOPIC_REGEX_DEFAULT = null;
    private static final String TOPIC_REGEX_DISPLAY = "topic regex";

    private static final String TOPIC_POLL_INTERVAL_MS_CONFIG = "topic.poll.interval.ms";
    private static final String TOPIC_POLL_INTERVAL_MS_DOC = "Frequency in ms to poll for new or removed topics, which may result in updated task "
            + "configurations to start polling for data in added topics/partitions or stop polling for data in "
            + "removed topics.";
    private static final int TOPIC_POLL_INTERVAL_MS_DEFAULT = 180000;
    private static final String TOPIC_POLL_INTERVAL_MS_DISPLAY = "Topic Config Sync Interval (ms)";

    private static final String TOPIC_RENAME_FORMAT_CONFIG = "topic.rename.format";
    private static final String TOPIC_RENAME_FORMAT_DOC = "A format string to rename the topics in the destination cluster. " +
            "the format string should contain '${topic}' that we bill replaced with the source topic name " +
            "For example, with'${topic}_mirror' format the topic 'test' will be renamed at the destination cluster to 'test_mirror'.";
    private static final String TOPIC_RENAME_FORMAT_DEFAULT = "${topic}";
    private static final String TOPIC_RENAME_FORMAT_DISPLAY = "topic regex";

    private static final String TOPIC_PRESERVE_PARTITIONS_CONFIG = "topic.preserve.partitions";
    private static final String TOPIC_PRESERVE_PARTITIONS_DOC = "Ensure that messages mirrored from the source cluster use " +
            "the same partition in the destination cluster. [if source topic have more partitions than destination topic, some partitions will be not mirrored.]";
    //private static final Boolean TOPIC_PRESERVE_PARTITIONS_DEFAULT = true;
    private static final String TOPIC_PRESERVE_PARTITIONS_DISPLAY = "Preserve Partitions";


    private static final String SOURCE_PREFIX = "src.kafka.";
    private static final String TARGET_PREFIX = "dest.kafka.";
    private static final String SRC_CONSUMER_PREFIX = "src.consumer.";
    private static final String ADMIN_CLIENT_PREFIX = "admin.";
    private static final String SOURCE_ADMIN_CLIENT_PREFIX = "source.admin.";
    private static final String TARGET_ADMIN_CLIENT_PREFIX = "target.admin.";

    private static void addConnectorOptions(ConfigDef configDef) {

        int orderInGroup = 0;
        String group;

        group = "connector";
        configDef.define(CONNECTOR_NAME_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.Importance.HIGH,
                CONNECTOR_NAME_DOC,
                group,
                ++orderInGroup,
                ConfigDef.Width.NONE,
                CONNECTOR_NAME_DISPLAY);

        group = "Source Topics";
        configDef.define(
                TOPIC_WHITELIST_CONFIG,
                ConfigDef.Type.LIST,
                Collections.emptyList(),
                ConfigDef.Importance.HIGH,
                TOPIC_WHITELIST_DOC,
                group,
                ++orderInGroup,
                ConfigDef.Width.NONE,
                TOPIC_WHITELIST_DISPLAY
        );
        configDef.define(
                TOPIC_BLACKLIST_CONFIG,
                ConfigDef.Type.LIST,
                Collections.emptyList(),
                ConfigDef.Importance.HIGH,
                TOPIC_BLACKLIST_DOC,
                group,
                ++orderInGroup,
                ConfigDef.Width.NONE,
                TOPIC_BLACKLIST_DISPLAY
        );
        configDef.define(
                TOPIC_REGEX_CONFIG,
                ConfigDef.Type.STRING,
                TOPIC_REGEX_DEFAULT,
                new RegexValidator(),
                ConfigDef.Importance.HIGH,
                TOPIC_REGEX_DOC,
                group,
                ++orderInGroup,
                ConfigDef.Width.NONE,
                TOPIC_REGEX_DISPLAY
        );

        configDef.define(
                TOPIC_POLL_INTERVAL_MS_CONFIG,
                ConfigDef.Type.INT,
                TOPIC_POLL_INTERVAL_MS_DEFAULT,
                ConfigDef.Range.atLeast(0),
                ConfigDef.Importance.LOW,
                TOPIC_POLL_INTERVAL_MS_DOC,
                group,
                ++orderInGroup,
                ConfigDef.Width.NONE,
                TOPIC_POLL_INTERVAL_MS_DISPLAY
        );

        orderInGroup = 0;
        group = "Destination Topics";
        configDef.define(
                TOPIC_RENAME_FORMAT_CONFIG,
                ConfigDef.Type.STRING,
                TOPIC_RENAME_FORMAT_DEFAULT,
                ConfigDef.Importance.HIGH,
                TOPIC_RENAME_FORMAT_DOC,
                group,
                ++orderInGroup,
                ConfigDef.Width.NONE,
                TOPIC_RENAME_FORMAT_DISPLAY
        );
        configDef.define(TOPIC_PRESERVE_PARTITIONS_CONFIG,
                ConfigDef.Type.BOOLEAN,
                true,
                ConfigDef.Importance.LOW,
                TOPIC_PRESERVE_PARTITIONS_DOC,
                group,
                ++orderInGroup,
                ConfigDef.Width.LONG,
                TOPIC_PRESERVE_PARTITIONS_DISPLAY
        );

    }


    // Getters

    String getConnectorName(){ return  getString(CONNECTOR_NAME_CONFIG); }

    Pattern getTopicPattern() {
        String regex = getString(TOPIC_REGEX_CONFIG);
        return regex == null ? null : Pattern.compile(regex);
    }

    Set<String> getWhiteListTopics() {
        return new HashSet<>(getList(TOPIC_WHITELIST_CONFIG));

    }

    Set<String> getBlackListTopics() {
        return new HashSet<>(getList(TOPIC_BLACKLIST_CONFIG));
    }


    public String getTopicRenameFormat() {
        return getString(TOPIC_RENAME_FORMAT_CONFIG);
    }

    int getTopicPollIntervalMs() {
        return getInt(TOPIC_POLL_INTERVAL_MS_CONFIG);
    }

    boolean getTopicPreservePartitions() {
        return getBoolean(TOPIC_PRESERVE_PARTITIONS_CONFIG);
    }

    // Consumer -----------------------------

    Map<String, Object> sourceConsumerConfig() {
        Map<String, Object> props = new HashMap<>();
        props.putAll(originalsWithPrefix(SOURCE_PREFIX));
        props.putAll(originalsWithPrefix(SRC_CONSUMER_PREFIX));
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        return props;
    }

    // Admin clients
    Map<String, Object> targetAdminClientConfig() {
        Map<String, Object> props = new HashMap<>();
        props.putAll(originalsWithPrefix(TARGET_PREFIX));
        props.putAll(originalsWithPrefix(ADMIN_CLIENT_PREFIX));
        props.putAll(originalsWithPrefix(TARGET_ADMIN_CLIENT_PREFIX));
        return props;
    }

    Map<String, Object> sourceAdminClientConfig() {
        Map<String, Object> props = new HashMap<>();
        props.putAll(originalsWithPrefix(SOURCE_PREFIX));
        props.putAll(originalsWithPrefix(ADMIN_CLIENT_PREFIX));
        props.putAll(originalsWithPrefix(SOURCE_ADMIN_CLIENT_PREFIX));
        return props;
    }

    List<MetricsReporter> metricsReporters() {
        List<MetricsReporter> reporters = new ArrayList<>();
        reporters.add(new JmxReporter("kafka.connect.mirror"));
        return reporters;
    }

    public static void main(String[] args) {
        System.out.println(CONFIG_DEF.toRst());
    }
}
