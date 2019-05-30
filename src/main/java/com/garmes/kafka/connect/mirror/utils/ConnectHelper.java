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

package com.garmes.kafka.connect.mirror.utils;

import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.TopicPartition;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ConnectHelper {

    public static Map<String, ?> toConnectPartition(TopicPartition topicPartition) {
        return toConnectPartition(topicPartition.topic(), topicPartition.partition());
    }

    public static Map<String, ?> toConnectPartition(String topic, int partition) {
        Map<String, Object> connectPartition = new HashMap<String, Object>(2);
        connectPartition.put("topic", topic);
        connectPartition.put("partition", partition);
        return connectPartition;
    }

    public static Map<String, Object> toConnectOffset(long offset) {
        return Collections.singletonMap("offset", offset);
    }

    public static String renameTopic(String renameFormat, String topic) {
        return renameFormat.replace("${topic}", topic);
    }

    public static  TopicPartition decodeTopicPartition(String topicPartitionString) {
        int sep = topicPartitionString.lastIndexOf('-');
        String topic = topicPartitionString.substring(0, sep);
        String partitionString = topicPartitionString.substring(sep + 1);
        int partition = Integer.parseInt(partitionString);
        return new TopicPartition(topic, partition);
    }

    public static String encodeTaskPartitions(List<TopicPartition> topicPartitions) {
        return topicPartitions.stream()
                .map(TopicPartition::toString)
                .collect(Collectors.joining(","));
    }

    public static boolean isInternalTopic(String topic) {
        return Topic.isInternal(topic);
    }

}
