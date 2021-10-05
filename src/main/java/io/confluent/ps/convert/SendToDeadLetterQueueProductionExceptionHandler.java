package io.confluent.ps.convert;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;


public class SendToDeadLetterQueueProductionExceptionHandler implements ProductionExceptionHandler {
    private final static Logger log = LoggerFactory.getLogger(SendToDeadLetterQueueDeserialicationExceptionHandler.class);
    KafkaProducer<byte[], byte[]> dlqProducer;
    String dlqTopic;

    @Override
    public ProductionExceptionHandlerResponse handle(final ProducerRecord<byte[], byte[]> record,
                                                     final Exception exception) {

        log.warn("Exception caught during Deserialization, sending to the dead queue topic; " +
                  "topic: {}, partition: {}",
                  record.topic(), record.partition(), exception);

        dlqProducer.send(new ProducerRecord<>(dlqTopic, null, record.timestamp(), record.key(), record.value()));

        return ProductionExceptionHandlerResponse.CONTINUE;
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        Properties props = new Properties();
        props.put("bootstrap.servers", configs.get("bootstrap.servers"));
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, configs.get("security.protocol"));

        dlqProducer = new KafkaProducer<byte[], byte[]>(props);
        dlqTopic = configs.get("error.topic.name").toString();
        log.info("Configured DLQ as topic: " + dlqTopic);
    }
}