package com.rideshare.trip.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Dead Letter Queue (DLQ) error handling configuration for trip event consumers.
 *
 * Design decisions:
 * - DefaultErrorHandler with exponential backoff (3 retries: 1s, 2s, 4s)
 * - DeadLetterPublishingRecoverer sends failed messages to events.dlq topic
 * - Preserves original message headers + exception details for debugging
 * - At-least-once delivery guarantee: message either succeeds or goes to DLQ
 *
 * Why DLQ?
 * - Prevents message loss when processing fails persistently (e.g., DB deadlock, constraint violation)
 * - Enables audit trail and manual recovery via dlq-processor service
 * - Avoids infinite retry loops that block partition processing
 */
@Configuration
public class KafkaErrorHandlingConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandlingConfig.class);
    private static final String DLQ_TOPIC = "events.dlq";

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /**
     * Producer factory for DLQ messages.
     * Uses StringSerializer for both key and value since we're forwarding the original record.
     */
    @Bean
    public ProducerFactory<String, Object> dlqProducerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        // Ensure DLQ messages are reliably written
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, Object> dlqKafkaTemplate() {
        return new KafkaTemplate<>(dlqProducerFactory());
    }

    /**
     * Dead letter publishing recoverer - sends failed messages to events.dlq topic.
     * Includes original message headers + exception details.
     */
    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(
            KafkaTemplate<String, Object> dlqKafkaTemplate) {

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
            dlqKafkaTemplate,
            (consumerRecord, exception) -> {
                log.error("Trip event processing failed after retries - sending to DLQ. " +
                    "Topic: {}, Partition: {}, Offset: {}, Key: {}, Exception: {}",
                    consumerRecord.topic(),
                    consumerRecord.partition(),
                    consumerRecord.offset(),
                    consumerRecord.key(),
                    exception.getMessage(),
                    exception);

                // Route all failed messages to events.dlq topic (partition 0 since DLQ has 1 partition)
                return new org.apache.kafka.common.TopicPartition(DLQ_TOPIC, 0);
            }
        );

        return recoverer;
    }

    /**
     * Error handler with exponential backoff retry policy.
     *
     * Retry schedule:
     * - Attempt 1: immediate
     * - Attempt 2: 1 second later
     * - Attempt 3: 2 seconds later (cumulative backoff)
     * - Attempt 4: 4 seconds later (cumulative backoff)
     * - After 3 retries: send to DLQ via DeadLetterPublishingRecoverer
     *
     * Why exponential backoff?
     * - Gives transient failures (DB connection timeout, deadlock) time to recover
     * - Reduces load during outages
     * - Prevents thundering herd when many messages fail simultaneously
     */
    @Bean
    public CommonErrorHandler kafkaErrorHandler(
            DeadLetterPublishingRecoverer deadLetterPublishingRecoverer) {

        // Configure exponential backoff: initial 1s, multiplier 2x, max 3 retries
        ExponentialBackOff backOff = new ExponentialBackOff(1000, 2);
        backOff.setMaxElapsedTime(7000); // Stop retrying after ~7 seconds total

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
            deadLetterPublishingRecoverer,
            backOff
        );

        // Log each retry attempt for observability
        errorHandler.setRetryListeners((record, ex, deliveryAttempt) -> {
            log.warn("Retry attempt {} for trip event: topic={}, partition={}, offset={}, key={}. Error: {}",
                deliveryAttempt,
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                ex.getMessage());
        });

        log.info("Trip event error handler configured with exponential backoff (3 retries) and DLQ topic: {}",
            DLQ_TOPIC);

        return errorHandler;
    }
}
