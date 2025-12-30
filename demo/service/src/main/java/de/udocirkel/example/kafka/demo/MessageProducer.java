package de.udocirkel.example.kafka.demo;

import lombok.RequiredArgsConstructor;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class MessageProducer {

    private final KafkaTemplate<String, String> kafka;

    public void send(String topic, String message) {
        kafka.send(topic, message);
    }

}
