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
import com.garmes.kafka.connect.mirror.utils.RegexValidator;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.lang.reflect.Field;
import java.util.*;
import java.util.regex.Pattern;

public class MirrorSourceConnectorConfig extends AbstractConfig {



    protected MirrorSourceConnectorConfig(ConfigDef subclassConfigDef, Map<String, String> props) {
        super(subclassConfigDef, props);
    }

    public MirrorSourceConnectorConfig(Map<String, String> props) {
        super(CONFIG_DEF, props);
    }

    public static final ConfigDef CONFIG_DEF = baseConfigDef();

    public static ConfigDef baseConfigDef() {
        ConfigDef config = new ConfigDef();
        addConnectorOptions(config);
        defineConsumersConf(SOURCE_PREFIX, SRC_CONSUMER_PREFIX,config);
        defineConsumersConf(DESTINATION_PREFIX, DEST_CONSUMER_PREFIX,config);
        return config;
    }

    public static final String CONNECTOR_NAME_CONFIG = "name";
    public static final String CONNECTOR_NAME_DOC = "Unique connector name";
    public static final String CONNECTOR_NAME_DISPLAY = "connector name";

    public static final String TOPIC_WHITELIST_CONFIG = "topic.whitelist";
    public static final String TOPIC_WHITELIST_DOC = "Whitelist of topics to be mirrored.";
    //public static final List TOPIC_WHITELIST_DEFAULT = Collections.emptyList();
    public static final String TOPIC_WHITELIST_DISPLAY = "topic whitelist";

    public static final String TOPIC_BLACKLIST_CONFIG = "topic.blacklist";
    public static final String TOPIC_BLACKLIST_DOC = "Topics to exclude from mirroring.";
    //public static final List TOPIC_BLACKLIST_DEFAULT = Collections.emptyList();
    public static final String TOPIC_BLACKLIST_DISPLAY = "topic blacklist";

    public static final String TOPIC_REGEX_CONFIG = "topic.regex";
    public static final String TOPIC_REGEX_DOC = "Regex of topics to mirror.";
    public static final String TOPIC_REGEX_DEFAULT = null;
    public static final String TOPIC_REGEX_DISPLAY = "topic regex";

    public static final String TOPIC_POLL_INTERVAL_MS_CONFIG = "topic.poll.interval.ms";
    public static final String TOPIC_POLL_INTERVAL_MS_DOC = "Frequency in ms to poll for new or removed topics, which may result in updated task "
            + "configurations to start polling for data in added topics/partitions or stop polling for data in "
            + "removed topics.";
    public static final int TOPIC_POLL_INTERVAL_MS_DEFAULT = 180000;
    public static final String TOPIC_POLL_INTERVAL_MS_DISPLAY = "Topic Config Sync Interval (ms)";

    public static final String TOPIC_RENAME_FORMAT_CONFIG = "topic.rename.format";
    public static final String TOPIC_RENAME_FORMAT_DOC = "A format string to rename the topics in the destination cluster. " +
            "the format string should contain '${topic}' that we bill replaced with the source topic name " +
            "For example, with'${topic}_mirror' format the topic 'test' will be renamed at the destination cluster to 'test_mirror'.";
    public static final String TOPIC_RENAME_FORMAT_DEFAULT = "${topic}";
    public static final String TOPIC_RENAME_FORMAT_DISPLAY = "topic regex";

    public static final String TOPIC_PRESERVE_PARTITIONS_CONFIG = "topic.preserve.partitions";
    public static final String TOPIC_PRESERVE_PARTITIONS_DOC = "Ensure that messages mirrored from the source cluster use " +
            "the same partition in the destination cluster. [if source topic have more partitions than destination topic, some partitions will be not mirrored.]";
    public static final Boolean TOPIC_PRESERVE_PARTITIONS_DEFAULT = true;
    public static final String TOPIC_PRESERVE_PARTITIONS_DISPLAY = "Preserve Partitions";


    public static final String SRC_CONSUMER_PREFIX = "src.consumer.";
    public static final String DEST_CONSUMER_PREFIX = "dest.consumer.";
    public static final String SOURCE_PREFIX = "src.kafka.";
    public static final String DESTINATION_PREFIX = "dest.kafka.";

    protected static void addConnectorOptions(ConfigDef configDef) {

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

    public String getConnectorName(){ return  getString(CONNECTOR_NAME_CONFIG); }

    public Pattern getTopicPattern() {
        String regex = getString(TOPIC_REGEX_CONFIG);
        return regex == null ? null : Pattern.compile(regex);
    }

    public Set<String> getWhiteListTopics() {
        return new HashSet(getList(TOPIC_WHITELIST_CONFIG));
    }

    public Set<String> getBlackListTopics() {
        return new HashSet(getList(TOPIC_BLACKLIST_CONFIG));
    }

    public String getTopicRenameFormat() {
        return getString(TOPIC_RENAME_FORMAT_CONFIG);
    }

    public int getTopicPollIntervalMs() {
        return getInt(TOPIC_POLL_INTERVAL_MS_CONFIG);
    }

    public boolean getTopicPreservePartitions() {
        return getBoolean(TOPIC_PRESERVE_PARTITIONS_CONFIG);
    }

    // Consumers -----------------------------
    public Map<String, ?> getSrcConsumerConfigs() {
        Map<String, Object> configs = originalsWithPrefix(SOURCE_PREFIX);
        configs.putAll(originalsWithPrefix(SRC_CONSUMER_PREFIX));
        return configs;
    }

    public Map<String, ?> getDestConsumerConfigs() {
        Map<String, Object> configs = originalsWithPrefix(DESTINATION_PREFIX);
        configs.putAll(originalsWithPrefix(DEST_CONSUMER_PREFIX));
        return configs;
    }


    private static void defineConsumersConf(String srcOrDest,String srcOrDestPrefix,ConfigDef configDef) {
        List<String> remove = new ArrayList<>();
        remove.add("key.deserializer");
        remove.add("value.deserializer");
        remove.add("bootstrap.servers");

        String group = srcOrDest+" Kafka: Consumer";
        int orderInGroup = 0;

        for (Map.Entry<String, ConfigDef.ConfigKey> keyEntry : ConnectHelper.consumerConfigs().entrySet()) {
            ConfigDef.ConfigKey value = keyEntry.getValue();
            if(remove.contains(keyEntry.getKey()))continue;
            configDef.define(srcOrDestPrefix + value.name,
                    value.type, value.defaultValue,
                    value.validator, value.importance,
                    value.documentation, group, ++orderInGroup,
                    value.width, value.displayName, value.recommender);
        }

    }


    public static void main(String[] args) {
        System.out.println(CONFIG_DEF.toRst());
    }
}
