package io.confluent.ps.convert;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;


public class SendToDeadLetterQueueDeserialicationExceptionHandler implements DeserializationExceptionHandler {
    private final static Logger log = LoggerFactory.getLogger(SendToDeadLetterQueueDeserialicationExceptionHandler.class);
    KafkaProducer<byte[], byte[]> dlqProducer;
    String dlqTopic;

    @Override
    public DeserializationHandlerResponse handle(final ProcessorContext context,
                                                 final ConsumerRecord<byte[], byte[]> record,
                                                 final Exception exception) {

        log.warn("Exception caught during Deserialization, sending to the dead queue topic; " +
                  "taskId: {}, topic: {}, partition: {}, offset: {}",
                  context.taskId(), record.topic(), record.partition(), record.offset(),
                  exception);

        dlqProducer.send(new ProducerRecord<>(dlqTopic, null, record.timestamp(), record.key(), record.value()));

        return DeserializationHandlerResponse.CONTINUE;
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