package com.rideshare.trip.config;

import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Routes trip.events records that repeatedly fail deserialization or processing to events.dlq
 * instead of blocking the partition or crashing the consumer. Spring Boot auto-wires the single
 * CommonErrorHandler bean in the context into its autoconfigured listener container factory.
 */
@Configuration
public class KafkaDlqConfig {

    @Bean
    public CommonErrorHandler tripEventErrorHandler(KafkaTemplate<Object, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
            kafkaTemplate,
            (record, ex) -> new TopicPartition("events.dlq", -1)
        );

        // Retry twice with a 1s backoff before giving up and publishing to the DLQ
        return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2));
    }
}
