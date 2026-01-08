package de.udocirkel.example.kafka.demo;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;
import static org.springframework.http.MediaType.TEXT_PLAIN_VALUE;

import java.util.List;

import lombok.RequiredArgsConstructor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/messages")
@RequiredArgsConstructor
public class MessageController {

    private static final Logger LOG = LoggerFactory.getLogger(MessageController.class);

    private final MessageProducer producer;

    private final MessageReader reader;

    @Value("${app.topic.name}")
    private String topic;

    @PostMapping(
            produces = TEXT_PLAIN_VALUE,
            consumes = TEXT_PLAIN_VALUE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public String send(@RequestBody String message) {
        LOG.debug("Sending message to topic '{}': {}", topic, message);
        producer.send(topic, message);
        return "ok";
    }

    @GetMapping(
            produces = APPLICATION_JSON_VALUE)
    public List<MessageRecord> readFromOffset(
            @RequestParam(name = "partition") int partition,
            @RequestParam(name = "offset") long offset,
            @RequestParam(name = "limit", defaultValue = "100") int limit) {
        LOG.debug("Reading messages from topic '{}' with offset {} and limit {}", topic, offset, limit);
        return reader.readFromOffset(topic, partition, offset, limit);
    }

}
