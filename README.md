# filebeats-message-extractor
Parse a filebeats kafka event. Split the message and the metadata in the event. Route the metadata into a compacted topic, and the message to another topic. Using the same key which is unique to the metadata, this allows the data to be re joined, if desired.

![filebeat-graphic](https://user-images.githubusercontent.com/5939204/136263847-2a89f40c-78e6-474a-b3fd-8fb343dafb89.png)

### Build and Execution Environment
* Java 8
* Confluent Platform 6.1 or newer

## Build
Use Maven to build the KStream Application.

```
mvn clean package
```

A successful build will create a target directory with the following two jar files:
* filebeats-message-extractor-0.1.jar
* filebeats-message-extractor-with-dependencies.jar

The `filebeats-message-extractor-with-dependencies.jar` file contains all the dependencies needed to run the application. Therefore, this dependencies jar file should be the file executed when running the KStream Application.

## Configuration
The KStream Application requires a configuration properties file.

Example:
```
application.id=filebeats-message-extractor
bootstrap.servers=localhost:9092
security.protocol=PLAINTEXT

# topic and table name configuration
input.topic.name=filebeats-sample-data
msg.topic.name=filebeats-messages-only
metadata.topic.name=filebeats-metadata
error.topic.name=filebeats-message-extractor-error

# message event configuration
msg.field.paths=/_id:filebeats_id, /fields/message:message, /fields/@timestamp:timestamp
msg.field.name=message

```

With the above configuration, the KStreams application will connect to the Kafka Brokers identified by the `bootstrap.servers` cluster making use of the `security.protocol` configuration. The KStreams Application will use a consumer group with the `application.id` and read its input from `input.topic.name` and write out the parsed message events to `msg.topic.name`, and the metadata to a compacted topic named `filebeats_metadata`. If any configured exceptions are caught with the `input.topic.name` deserialization or parsing, the event will not be written to `msg.topic.name`, and will instead be written to `error.topic.name`. To horizontally scale the KStream, make sure the `input.topic.name` has multiple partitions and start another jvm with the same configuration properties file.


The elements that generate the lightweight message event are configurable in the `msg.field.paths` property.  This is a comma seperated list of JSONPointers and names ( \<pointer-to-element\>:\<new-name\> ). So for example, if the source json is `{"object":{"msg":"test"}}` if you want the message event to use the json element called `msg`, and you want it be be named `message` in the new event, then you would used `/object/msg:message` 
 
Here is additional information on how to construct a JSONPointer:
 
![Screen Shot 2021-10-06 at 2 54 03 PM](https://user-images.githubusercontent.com/5939204/136265486-fdf6cd9d-5dc8-4f21-a4c2-6e4b609bce91.png)

## Execution
Run the `filebeats-message-extractor-0.1-jar-with-dependencies.jar` with Java 8.

```
 java -jar filebeats-message-extractor-0.1-jar-with-dependencies.jar configuration/dev.properties
```

## Testing/Demo example

#### 1. Create the topics
Note: The `partitions` and `segment.bytes` configuration on the metadata topic is set low for demo purposes only. In production, you will want to configure the partitions and log cleaner compaction less aggressively.
```
 kafka-topics --bootstrap-server localhost:9092 --create --topic filebeats-sample-data --replication-factor 1 --partitions 1 
 kafka-topics --bootstrap-server localhost:9092 --create --topic filebeats-messages-only --replication-factor 1 --partitions 1
 kafka-topics --bootstrap-server localhost:9092 --create --topic filebeats-metadata --replication-factor 1 --partitions 1 --config "cleanup.policy=compact" --config "delete.retention.ms=100" --config "segment.ms=100" --config "segment.bytes=16000" --config "min.cleanable.dirty.ratio=0.01"
 kafka-topics --bootstrap-server localhost:9092 --create --topic filebeats-message-extractor-error --replication-factor 1 --partitions 1 
```

#### 2. Push the sample filebeats data to the input topic
```
cat test/resources/filebeats.data | kcat -b localhost:9092 -P -t filebeats-sample-data -D '!'
```

#### 3. Start the Kafka Stream App
Reference the Configuration and Execution sections above.
```
 java -jar target/filebeats-message-extractor-0.1-jar-with-dependencies.jar configuration/dev.properties
```


#### 5. Validation
There are 11 messages in the `filebeats.data` sample.  If you inspect each topic after running this stream with either the Confluent Control Center or a command line consumer, you should find the following results:
```
kafka-console-consumer --bootstrap-server=localhost:9092 --topic filebeats-metadata --from-beginning
```
* filebeats-sample-data topic = 11 messages
* filebeats-message-only topic = 10 messages
* filebeats-metadata topic = 4 messages
* filebeats-message-extractor-error topic = 1 message
