package de.udocirkel.example.kafka.demo;

import java.time.Duration;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class MessageReader {

    private static final String READER_GROUP_ID = "demo-rest-reader-group";
    private static final int DEFAULT_PARTITION = 0;

    private final Properties consumerProps;

    public MessageReader(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        this.consumerProps = createConsumerProperties(bootstrapServers);
    }

    private Properties createConsumerProperties(String bootstrapServers) {
        var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, READER_GROUP_ID);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        return props;
    }

    public List<MessageRecord> readFromOffset(String topic, long offset, int limit) {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {

            var partition = new TopicPartition(topic, DEFAULT_PARTITION);
            consumer.assign(List.of(partition));
            consumer.seek(partition, offset);

            var result = new ArrayList<MessageRecord>();
            while (result.size() < limit) {

                var records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> r : records) {
                    result.add(new MessageRecord(r.offset(), r.value()));
                    if (result.size() >= limit) {
                        break;
                    }
                }
            }

            return result;
        }
    }

}
