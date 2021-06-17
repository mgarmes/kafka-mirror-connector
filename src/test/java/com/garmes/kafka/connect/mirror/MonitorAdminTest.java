package com.garmes.kafka.connect.mirror;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class MonitorAdminTest {

    @Test
    public void topicToNotReplicateTest() {
        Set<String> sourceTopics = new HashSet<>();
        sourceTopics.add("test0");
        sourceTopics.add("test1");
        sourceTopics.add("test2");
        sourceTopics.add("test3");

        Set<String> targetTopics = new HashSet<>();
        targetTopics.add("test0_x");
        targetTopics.add("test2_x");

        Set<String> expectedTopicsToNotReplicate = new HashSet<>();
        expectedTopicsToNotReplicate.add("test1");
        expectedTopicsToNotReplicate.add("test3");

        MonitorAdmin monitorAdmin = new MonitorAdmin(null, "AtoB",
                new HashSet<String>(), new HashSet<String>(), Pattern.compile(".*"), 5000, null, null, "${topic}_x", true);

        Set<String> topicsToNotReplicate = monitorAdmin.topicsToNotReplicate(sourceTopics, targetTopics);


        assertEquals(expectedTopicsToNotReplicate, topicsToNotReplicate);

    }

    @Test
    public void shouldReplicateTopicTest() {
        Set<String> blackListedTopics = new HashSet<>();
        blackListedTopics.add("app0");

        MonitorAdmin monitorAdmin = new MonitorAdmin(null, "AtoB",
                blackListedTopics, new HashSet<String>(), Pattern.compile("app.*"), 5000, null, null, "${topic}_x", true);

        assertEquals(false, monitorAdmin.shouldReplicateTopic("app0"));
        assertEquals(true, monitorAdmin.shouldReplicateTopic("app2"));
        assertEquals(true, monitorAdmin.shouldReplicateTopic("appxxxx"));
        assertEquals(false, monitorAdmin.shouldReplicateTopic("test_app"));

    }




}
