package de.udocirkel.example.kafka.demo;

import java.util.Optional;

public record MessageRecord(
        String topic,
        String key,
        int partition,
        Optional<Integer> leaderEpoch,
        long offset,
        long timestamp,
        String value) {
}
