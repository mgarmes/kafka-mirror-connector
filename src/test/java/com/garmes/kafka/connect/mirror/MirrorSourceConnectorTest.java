package com.garmes.kafka.connect.mirror;

import com.garmes.kafka.connect.mirror.utils.ConnectHelper;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;



public class MirrorSourceConnectorTest {

    @Test
    public void topicRenameTest() {
        String sourceTopic = "test";
        assertEquals("test", ConnectHelper.renameTopic("${topic}", sourceTopic));
        assertEquals("test_x", ConnectHelper.renameTopic("${topic}_x", sourceTopic));
        assertEquals("x.test", ConnectHelper.renameTopic("x.${topic}", sourceTopic));
        assertEquals("x.test_x", ConnectHelper.renameTopic("x.${topic}_x", sourceTopic));
    }

    @Test
    public void testConsumerConfig() {
        Map<String, String> connectorProps = new HashMap<>();
        connectorProps.put("name", "AtoB");
        connectorProps.put(MirrorSourceConnectorConfig.SRC_CONSUMER_PREFIX+ "max.poll.interval.ms", "120000");

        MirrorSourceConnectorConfig config = new MirrorSourceConnectorConfig(connectorProps);
        Map<String, Object> connectorConsumerProps = config.sourceConsumerConfig();
        Map<String, Object> expectedConsumerProps = new HashMap<>();
        expectedConsumerProps.put("enable.auto.commit", "false");
        expectedConsumerProps.put("auto.offset.reset", "earliest");
        expectedConsumerProps.put("max.poll.interval.ms", "120000");
        assertEquals(expectedConsumerProps, connectorConsumerProps);

        // checking auto.offset.reset override works
        connectorProps = new HashMap<>();
        connectorProps.put("name", "AtoB");
        connectorProps.put(MirrorSourceConnectorConfig.SRC_CONSUMER_PREFIX+ "auto.offset.reset", "latest");

        config = new MirrorSourceConnectorConfig(connectorProps);
        connectorConsumerProps = config.sourceConsumerConfig();
        expectedConsumerProps.put("auto.offset.reset", "earliest");
        expectedConsumerProps.remove("max.poll.interval.ms");
        assertEquals(expectedConsumerProps, connectorConsumerProps,
                MirrorSourceConnectorConfig.SRC_CONSUMER_PREFIX + " source consumer config not matching");
    }

    @Test
    public void testAdminConfig() {
        Map<String, String> connectorProps = new HashMap<>();
        connectorProps.put("name", "AtoB");
        connectorProps.put(MirrorSourceConnectorConfig.ADMIN_CLIENT_PREFIX + "max.poll.interval.ms", "120000");
        connectorProps.put(MirrorSourceConnectorConfig.SOURCE_ADMIN_CLIENT_PREFIX + "bootstrap.servers", "kafka.source:9092");
        connectorProps.put(MirrorSourceConnectorConfig.TARGET_ADMIN_CLIENT_PREFIX + "bootstrap.servers", "kafka.target:9092");

        MirrorSourceConnectorConfig config = new MirrorSourceConnectorConfig(connectorProps);
        Map<String, Object> adminSourceClientConfig = config.sourceAdminClientConfig();
        Map<String, Object> adminTargetClientConfig = config.targetAdminClientConfig();

        Map<String, Object> expectedAdminSourceClientConfig = new HashMap<>();
        expectedAdminSourceClientConfig.put("max.poll.interval.ms", "120000");
        expectedAdminSourceClientConfig.put("bootstrap.servers", "kafka.source:9092");
        assertEquals(expectedAdminSourceClientConfig, adminSourceClientConfig);


        Map<String, Object> expectedAdminTargetClientConfig = new HashMap<>();
        expectedAdminTargetClientConfig.put("max.poll.interval.ms", "120000");
        expectedAdminTargetClientConfig.put("bootstrap.servers", "kafka.target:9092");
        assertEquals(expectedAdminTargetClientConfig, adminTargetClientConfig);
    }

}
