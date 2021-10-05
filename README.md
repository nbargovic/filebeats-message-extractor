# filebeats-message-extractor
Parse a filebeats kafka event. Split the message and the metadata in the event. Route the metadata into a KTable, and the message to another topic.

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

##Configuration
The KStream Application requires a configuration properties file.

Example:
```
application.id=filebeats-message-extractor
bootstrap.servers=localhost:9092
security.protocol=PLAINTEXT

# topic and table name configuration
input.topic.name=filebeats-sample-data
msg.topic.name=filebeats-messages-only
metadata.table.changelog.suffix=metadata
error.topic.name=filebeats-message-extractor-error

# message event configuration
msg.field.paths=/_id:filebeats_id, /fields/message:message, /fields/@timestamp:timestamp
msg.field.name=message
meta.root.path=/fields

```

With the above configuration, the KStreams application will connect to the Kafka Brokers identified by the `bootstrap.servers` cluster making use of the `security.protocol` configuration. The KStreams Application will use a consumer group with the `application.id` and read its input from `input.topic.name` and write out the parsed message events to `msg.topic.name`, and the metadata to changelog stream that back a KTable named `filebeats_metadata`. If any configured exceptions are caught with the `input.topic.name` deserialization or parsing, the event will not be written to `msg.topic.name`, and will instead be written to `error.topic.name`. To horizontally scale the KStream, make sure the `input.topic.name` has multiple partitions and start another jvm with the same configuration properties file.

## Execution
Run the `filebeats-message-extractor-0.1-jar-with-dependencies.jar` with Java 8.

```
 java -jar filebeats-message-extractor-0.1-jar-with-dependencies.jar configuration/dev.properties
```

## Testing example

#### 1. Create the topics
```
 kafka-topics --bootstrap-server localhost:9092 --create --topic filebeats-sample-data --replication-factor 1 --partitions 3 
 kafka-topics --bootstrap-server localhost:9092 --create --topic filebeats-messages-only --replication-factor 1 --partitions 3
 kafka-topics --bootstrap-server localhost:9092 --create --topic filebeats-message-extractor-error --replication-factor 1 --partitions 3 
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

#### 4. Create the KTable
You can use these ksqlDB commands in the Confluent Control Center editor to query the data in the KTable.

NOTE! - Be sure to set `auto.offset.reset = earliest` when executing this.
```
CREATE OR REPLACE TABLE filebeats_metadata
(id 					VARCHAR PRIMARY KEY,
"agent.version.keyword"			Array<VARCHAR>,
"host.architecture.keyword"		Array<VARCHAR>,
"host.name.keyword"			Array<VARCHAR>,
"host.os.build.keyword"			Array<VARCHAR>,
"host.hostname"				Array<VARCHAR>,
"host.mac"				Array<VARCHAR>,
"agent.hostname.keyword"		Array<VARCHAR>,
"ecs.version.keyword"		        Array<VARCHAR>,
"host.ip.keyword"			Array<VARCHAR>,
"host.os.version"			Array<VARCHAR>,
"host.os.name"				Array<VARCHAR>,
"agent.name"				Array<VARCHAR>,
"host.id.keyword"			Array<VARCHAR>,
"host.name"				Array<VARCHAR>,
"host.os.version.keyword"	        Array<VARCHAR>,
"agent.id.keyword"			Array<VARCHAR>,
"input.type"				Array<VARCHAR>,
"@version.keyword"			Array<VARCHAR>,
"log.offset"				Array<VARCHAR>,
"log.flags"				Array<VARCHAR>,
"agent.hostname"			Array<VARCHAR>,
"host.architecture"			Array<VARCHAR>,
"agent.id"				Array<VARCHAR>,
"ecs.version"				Array<VARCHAR>,
"host.hostname.keyword"			Array<VARCHAR>,
"agent.version"				Array<VARCHAR>,
"host.os.family"			Array<VARCHAR>,
"input.type.keyword"			Array<VARCHAR>,
"host.os.build"				Array<VARCHAR>,
"host.ip"				Array<VARCHAR>,
"agent.type"				Array<VARCHAR>,
"host.os.kernel.keyword"		Array<VARCHAR>,
"log.flags.keyword"			Array<VARCHAR>,
"host.os.kernel"			Array<VARCHAR>,
"@version"				Array<VARCHAR>,
"host.os.name.keyword"			Array<VARCHAR>,
"host.id"				Array<VARCHAR>,
"log.file.path.keyword" 		Array<VARCHAR>,
"agent.type.keyword"			Array<VARCHAR>,
"agent.ephemeral_id.keyword"            Array<VARCHAR>,
"host.mac.keyword"			Array<VARCHAR>,
"agent.name.keyword"			Array<VARCHAR>,
"host.os.family.keyword"	        Array<VARCHAR>,
"host.os.platform"			Array<VARCHAR>,
"host.os.platform.keyword"		Array<VARCHAR>,
"log.file.path"				Array<VARCHAR>,
"agent.ephemeral_id"			Array<VARCHAR>)
WITH (KAFKA_TOPIC = 'filebeats-message-extractor-metadata-changelog',
      VALUE_FORMAT='JSON');
      
SELECT * FROM filebeats_metadata EMIT CHANGES;      
```

#### 5. Validation