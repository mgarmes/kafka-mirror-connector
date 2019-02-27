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
import org.apache.commons.codec.binary.Base64;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigDef;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    /*
    possible solution so convert the byte array to String
    1 - "ISO-8859-1"
    2 - hexadecimal
    3 - base64
    Base64 expands the size of data compared to its binary form by one third. So your 40 MB file will be about 53 MB.
    Hex encoding doubles the size of the data, so your 40 MB file will be 80 MB.
     */
    public static PartitionAssignor.Assignment decodeTaskPartition(String encodedAssignment) {
        return ConsumerProtocol.deserializeAssignment(ByteBuffer.wrap(Base64.decodeBase64(encodedAssignment)));
    }

    public static String encodeTaskPartitions(PartitionAssignor.Assignment assignment) {
        //convert the serialized assignment ByteBuffer to String (task config is a String Object)
        return Base64.encodeBase64String(ConsumerProtocol.serializeAssignment(assignment).array());
    }

    public static boolean isInternalTopic(String topic) {
        return Topic.isInternal(topic);
    }

    public static String topicMetadataToString(Map<String, List<PartitionInfo>> metadata){
        StringBuilder stringBuilder = new StringBuilder();

        for(Map.Entry<String, List<PartitionInfo>> entry: metadata.entrySet()){
            stringBuilder.append(entry.getKey());
            stringBuilder.append(":[");
            for (PartitionInfo partitionInfo:entry.getValue()){
                stringBuilder.append(partitionInfo.partition()+",");
            }
            stringBuilder.append("],");
        }
        return stringBuilder.toString();
    }

    public static Map<String, ConfigDef.ConfigKey> consumerConfigs() {
        // Access to private field https://stackoverflow.com/questions/1196192/how-to-read-the-value-of-a-private-field-from-a-different-class-in-java
        try {
            Field field = ConsumerConfig.class.getDeclaredField("CONFIG");
            field.setAccessible(true);
            ConfigDef configDef = (ConfigDef) field.get(ConsumerConfig.class);
            return configDef.configKeys();
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
