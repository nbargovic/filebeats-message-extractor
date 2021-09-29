package io.confluent.ps.convert;

import static org.apache.kafka.common.serialization.Serdes.String;

import java.io.FileInputStream;
import java.io.IOException;

import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
* Parse a filebeats kafka event. Split the message and the metadata in the event. Route the metadata into a KTable,
* and the message to another topic.
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
        //props.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("application.id"));
        //props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, String().getClass());
        //props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, envProps.getProperty("security.protocol"));
        //props.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, envProps.getProperty("security.protocol"));
        //props.put(SaslConfigs.SASL_MECHANISM, envProps.getProperty("sasl.mechanism"));
        //props.put(SaslConfigs.SASL_JAAS_CONFIG, envProps.getProperty("sasl.jaas.config"));

        //log.debug("SASL Config------");
        //log.debug("bootstrap.servers={}", envProps.getProperty("bootstrap.servers"));
        //log.debug("security.protocol={}", envProps.getProperty("security.protocol"));
        //log.debug("sasl.mechanism={}", envProps.getProperty("sasl.mechanism"));
        //log.debug("sasl.jaas.config={}", envProps.getProperty("sasl.jaas.config"));
        //log.debug("-----------------");

        //props.put("error.topic.name", envProps.getProperty("error.topic.name"));
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
        final String outputTopicName = envProps.getProperty("output.topic.name");
        final String metadataTableName = envProps.getProperty("metadata.ktable.name");

        final StreamsBuilder builder = new StreamsBuilder();

        // Build the json Serialiser for log Entry
        //example from https://github.com/apache/kafka/blob/1.0/streams/examples/src/main/java/org/apache/kafka/streams/examples/pageview/PageViewTypedDemo.java
        Map<String, Object> serdeProps = new HashMap<>();
        final Serializer<String> fbSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", String.class);
        fbSerializer.configure(serdeProps, false);

        final Deserializer<String> fbDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", String.class);
        fbDeserializer.configure(serdeProps, false);

        final Serde<String> fbSerde = Serdes.serdeFrom(fbSerializer, fbDeserializer);

        final KStream<String, Bytes> beatsStream = builder.stream(inputTopicName, Consumed.with(Serdes.String(), Serdes.Bytes()));

        beatsStream.flatMap( (key, beatsData) -> {
                    List<KeyValue<String, String>> messages= new LinkedList<>();
                    String beatsValue = beatsData.toString();

                    //TODO implement message and metadata extract

                    return messages;
                });

                //TODO emit message to ouptput stream, and metadata to ktable

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
        //TODO - sample properties
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
