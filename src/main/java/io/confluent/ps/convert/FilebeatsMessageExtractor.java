package io.confluent.ps.convert;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import static org.apache.kafka.common.serialization.Serdes.String;

/**
* Parse a filebeats kafka event. Split the message and the metadata in the event. Route the metadata into a compacted topic,
* and the messages to another topic.
*/
public final class FilebeatsMessageExtractor {
    private final Logger log = LoggerFactory.getLogger(FilebeatsMessageExtractor.class);

    private FilebeatsMessageExtractor() {
    }

    /**
    * Setup the Streams Processors we will be using from the passed in configuration.properties.
    * @param envProps Environment Properties file
    * @return Properties Object ready for KafkaStreams Topology Builder
    */
    protected Properties buildStreamsProperties(Properties envProps) {
        Properties props = new Properties();
        props.putAll(envProps);

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, String().getClass());
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, SendToDeadLetterQueueDeserialicationExceptionHandler.class.getName());
        props.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, SendToDeadLetterQueueProductionExceptionHandler.class.getName());

        // Broken negative timestamp
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());

        props.put(StreamsConfig.PRODUCER_PREFIX + ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
            "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor");

        props.put(StreamsConfig.MAIN_CONSUMER_PREFIX + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
            "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor");

        return props;
    }

    /**
    * Load in the Environment Properties that were passed in from the CLI.
    * @param fileName
    * @return
    * @throws IOException
    */
    protected Properties loadEnvProperties(String fileName) throws IOException {
        Properties envProps = new Properties();

        try (
        FileInputStream input = new FileInputStream(fileName);
        ) {
            envProps.load(input);
        }
        return envProps;
    }

    /**
     * Build the topology from the loaded configuration
     * @param envProps built by the buildStreamsProperties
     * @return The build topology
     */
    protected Topology buildTopology(Properties envProps) {
        log.debug("Starting buildTopology");
        final String inputTopicName = envProps.getProperty("input.topic.name");
        final String msgTopicName = envProps.getProperty("msg.topic.name");
        final String metadataTopicName = envProps.getProperty("metadata.topic.name");
        final String[] msgPaths = envProps.getProperty("msg.field.paths").split(",");
        final String msgFieldName = envProps.getProperty("msg.field.name");

        final StreamsBuilder builder = new StreamsBuilder();

        Map<String, Object> serdeProps = new HashMap<>();
        final Serializer<JsonNode> fbSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", JsonNode.class);
        fbSerializer.configure(serdeProps, false);

        final Deserializer<JsonNode> fbDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", JsonNode.class);
        fbDeserializer.configure(serdeProps, false);

        final Serde<JsonNode> fbSerde = Serdes.serdeFrom(fbSerializer, fbDeserializer);

        final KStream<String, JsonNode> beatsStream = builder.stream(inputTopicName, Consumed.with(Serdes.String(), fbSerde));

        KStream<String, JsonNode> splits = beatsStream.flatMap( (key, beatsData) -> {

          log.debug("Deserialized input message.");

          List<KeyValue<String, JsonNode>> messages = new LinkedList<>();
          try {
            ObjectMapper mapper = new ObjectMapper();

            //parse the message field config values
            HashMap<String, JsonNode> msgNodes = new HashMap<>();     // key = new field name in message payload, value = the json node
            ArrayList<String> fieldsToRemove = new ArrayList<>();     // stash the metadata field paths to remove
            for ( String field : msgPaths){
                String[] pathAndName = field.split(":");
                msgNodes.put(pathAndName[1].trim(), beatsData.at(pathAndName[0].trim()));
                fieldsToRemove.add(pathAndName[0].trim());
            }

            log.info("Generating a message event with fields: " + fieldsToRemove.toArray().toString());

            //make the lightweight message data event
            JsonNode msgPayload = mapper.createObjectNode();
            ObjectNode msgObject = (ObjectNode)msgPayload;
            for (Map.Entry<String, JsonNode> entry : msgNodes.entrySet()){
                String value = entry.getValue().isTextual() ? entry.getValue().asText() : entry.getValue().toString();
                msgObject.put(entry.getKey(), value);
            }

            log.info("Message event constructed successfully.");

            //make the metadata event
            ObjectNode metaObject = (ObjectNode)beatsData;
            for( String path : fieldsToRemove) {
                JsonHasher.removeNode(metaObject, path);
            }
            JsonNode metaPayload = mapper.treeToValue(mapper.valueToTree(metaObject), JsonNode.class);

            log.info("Metadata event constructed successfully.");

            //create a shared id to be used as the primary key in the metadata ktable, and the key in the message topic
            String generatedKey = JsonHasher.generateHash(metaPayload);

            log.info("Generated hash id for message and metadata: " + generatedKey );

            messages.add(KeyValue.pair(generatedKey, msgPayload));
            messages.add(KeyValue.pair(generatedKey, metaPayload));

            log.debug("Message = {}", msgPayload.toString());
            log.debug("Metadata = {}", metaPayload.toString());

            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }

            return messages;
        });

        KStream<String, JsonNode>[] branches = splits.branch(
                (id, value) -> Objects.nonNull(value.get(msgFieldName)), //message
                (id, value) -> true                                      //metadata
        );

        branches[0].to(msgTopicName, Produced.with(Serdes.String(), fbSerde));
        branches[1].to(metadataTopicName, Produced.with(Serdes.String(), fbSerde));

        return builder.build();
    }

    /**
    * Main function that handles the life cycle of the Kafka Streams app.
    * @param configPath
    * @throws IOException
    */
    private void run(String configPath) throws IOException {

        Properties envProps = this.loadEnvProperties(configPath);
        Properties streamProps = this.buildStreamsProperties(envProps);

        Topology topology = this.buildTopology(envProps);

        final KafkaStreams streams = new KafkaStreams(topology, streamProps);
        final CountDownLatch latch = new CountDownLatch(1);

        // Attach shutdown handler to catch Control-C.
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close(Duration.ofSeconds(5));
                latch.countDown();
            }
        });

        try {
            streams.cleanUp();
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    private static void exampleProperties() {
        System.out.println("Please create a configuration properties file and pass it on the command line as an argument");
        System.out.println("Sample env.properties:");

        System.out.println("----------------------------------------------------------------");
        System.out.println("application.id=filebeats-message-extractor");
        System.out.println("bootstrap.servers=localhost:9092");
        System.out.println("security.protocol=PLAINTEXT");
        System.out.println("input.topic.name=filebeats-sample-data");
        System.out.println("msg.topic.name=filebeats-messages-only");
        System.out.println("metadata.table.changelog.suffix=metadata");
        System.out.println("error.topic.name=filebeats-message-extractor-error");
        System.out.println("msg.field.paths=/_id:filebeats_id, /fields/message:message, /fields/@timestamp:timestamp");
        System.out.println("msg.field.name=message");
        System.out.println("meta.root.path=/fields");
        System.out.println("----------------------------------------------------------------");
    }

    /**
    * Main method.
    * @param args - runtime properties file
    * @throws IOException
    */
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            exampleProperties();
            throw new IllegalArgumentException("This program takes one argument: the path to an environment configuration file.");
        }

        new FilebeatsMessageExtractor().run(args[0]);
    }
}
