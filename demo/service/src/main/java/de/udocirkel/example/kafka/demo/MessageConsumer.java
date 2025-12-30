package de.udocirkel.example.kafka.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class MessageConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(MessageConsumer.class);

    @KafkaListener(topics = "${app.topic}")
    public void receive(String message) {
        LOG.debug("Received message: '{}'", message);
    }

}
