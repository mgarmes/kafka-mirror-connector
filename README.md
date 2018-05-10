# Kafka Connect Mirror Connector

kafka-connect-mirror is a [Kafka Connector](http://kafka.apache.org/documentation.html#connect)
for mirroring data between Kafka clusters.

### Building

```bash
mvn clean package 
```

### Quick Start
#### connect standalone

```bash

echo "
# basic connector configuration
connector.name = dc1-to-dc2
connector.class = com.garmes.kafka.connect.mirror.MirrorSourceConnector

# source cluster connection info
src.kafka.bootstrap.servers = kafka_dc1:9092

# destination cluster connection info
dest.kafka.bootstrap.servers = kafka_dc2:9092

# configure mirrored topics
topic.whitelist = test

" > mirror.properties

./bin/connect-standalone.sh ../config/connect-standalone.properties  mirror.properties
```

#### connect distributed
```bash
 curl -X POST -H "Content-Type: application/json" --data '{"name": "dc1-to-dc2", "config": {"connector.class":"com.garmes.kafka.connect.mirror.MirrorSourceConnector", "tasks.max":"2", "src.kafka.bootstrap.servers":"kafka_dc1:9092", "dest.kafka.bootstrap.servers":"kafka_dc2:9092","topic.whitelist":"test"  }}' http://localhost:8083/connectors
```

### Configuration

``topic.whitelist``
  Whitelist of topics to be mirrored.

  * Type: list
  * Default: ""
  * Importance: high

``topic.blacklist``
  Topics to exclude from mirroring.

  * Type: list
  * Default: ""
  * Importance: high

``topic.regex``
  Regex of topics to mirror.

  * Type: string
  * Default: null
  * Valid Values: '.' for all topics
  * Importance: high

``topic.poll.interval.ms``
  Frequency in ms to poll for new or removed topics, which may result in updated task configurations to start polling for data in added topics/partitions or stop polling for data in removed topics.

  * Type: int
  * Default: 180000
  * Valid Values: [0,...]
  * Importance: low

``topic.rename.format``
  A format string to rename the topics in the destination cluster. the format string should contain '${topic}' that we bill replaced with the source topic name For example, with'${topic}_mirror' format the topic 'test' will be renamed at the destination cluster to 'test_mirror'.

  * Type: string
  * Default: ${topic}
  * Importance: high

``topic.preserve.partitions``
  Ensure that messages mirrored from the source cluster use the same partition in the destination cluster. [if source topic have more partitions than destination topic, some partitions will be not mirrored.]

  * Type: boolean
  * Default: true
  * Importance: low

