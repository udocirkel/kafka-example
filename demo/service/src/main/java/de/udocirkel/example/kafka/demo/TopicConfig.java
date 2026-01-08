package de.udocirkel.example.kafka.demo;

import org.apache.kafka.clients.admin.NewTopic;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class TopicConfig {

    @Value("${app.topic.name}")
    private String name;

    @Value("${app.topic.partitions}")
    private Integer partitions;

    @Value("${app.topic.replicas}")
    private Integer replicas;

    @Value("${app.topic.min-insync-replicas}")
    private String minInsyncReplicas;

    @Bean
    public NewTopic appTopic() {
        var t = TopicBuilder.name(name);
        if (partitions != null) {
            t.partitions(partitions);
        }
        if (replicas != null) {
            t.replicas(replicas);
        }
        if (minInsyncReplicas != null) {
            t.config("min.insync.replicas", minInsyncReplicas);
        }
        return t.build();
    }

}
