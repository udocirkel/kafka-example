package de.udocirkel.example.kafka.demo;

import lombok.RequiredArgsConstructor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicInteger;

@Service
@RequiredArgsConstructor
public class MessageProducer {

    private static final Logger LOG = LoggerFactory.getLogger(MessageProducer.class);

    private final KafkaTemplate<String, String> kafka;

    private final AtomicInteger counter = new AtomicInteger(0);

    public void send(String topic, String message) {
        var id = counter.incrementAndGet();
        var messageWithId = message + " (" + id + ")";
        kafka
                .send(topic, messageWithId, messageWithId)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        LOG.error("Sent message to topic '{}' failed: value={}", topic, messageWithId, ex);
                    } else {
                        var pm = result.getProducerRecord();
                        var rm = result.getRecordMetadata();
                        LOG.debug("Sent message to topic '{}' successful:  partition={}, key-size={}, value-size={}, value={}",
                                rm.topic(), rm.partition(), rm.serializedKeySize(), rm.serializedValueSize(), pm.value());
                    }
                });
    }

}
