package com.rideshare.gps.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.Map;

/**
 * Kafka topic configuration.
 * Creates all required topics on application startup if they don't exist.
 */
@Configuration
public class KafkaTopicConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public KafkaAdmin kafkaAdmin() {
        return new KafkaAdmin(Map.of(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers
        ));
    }

    /**
     * driver.location topic - 8 partitions for horizontal scalability
     * Keyed by driverId to guarantee ordering per driver
     */
    @Bean
    public NewTopic driverLocationTopic() {
        return TopicBuilder.name("driver.location")
            .partitions(8)
            .replicas(1)
            .build();
    }

    /**
     * trip.events topic - 4 partitions
     * Keyed by tripId for ordering guarantees per trip
     */
    @Bean
    public NewTopic tripEventsTopic() {
        return TopicBuilder.name("trip.events")
            .partitions(4)
            .replicas(1)
            .build();
    }

    /**
     * surge.pricing topic - 2 partitions
     * Keyed by zoneId for ordering guarantees per zone
     */
    @Bean
    public NewTopic surgePricingTopic() {
        return TopicBuilder.name("surge.pricing")
            .partitions(2)
            .replicas(1)
            .build();
    }

    /**
     * events.dlq topic - 1 partition (dead letter queue)
     * All failed events from all consumers go here
     */
    @Bean
    public NewTopic deadLetterQueueTopic() {
        return TopicBuilder.name("events.dlq")
            .partitions(1)
            .replicas(1)
            .build();
    }
}
