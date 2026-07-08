package com.rideshare.location.config;

import com.rideshare.location.model.DriverLocationEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka consumer configuration with manual offset management.
 *
 * Key settings:
 * - enable.auto.commit = false: Manual offset commit for at-least-once delivery
 * - AckMode.MANUAL: Application controls when to commit offsets via Acknowledgment.acknowledge()
 * - max.poll.records = 100: Process 100 records at a time (tunable for throughput vs latency)
 *
 * Why manual commit?
 * - Ensures Redis write completes before offset is committed
 * - Prevents data loss if consumer crashes after Kafka auto-commit but before Redis write
 * - Foundation for exactly-once semantics (Phase 4)
 */
@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Bean
    public ConsumerFactory<String, DriverLocationEvent> consumerFactory() {
        Map<String, Object> config = new HashMap<>();

        // Broker connection
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // Consumer group
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // Disable auto-commit - we'll commit manually after Redis write
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // Deserializers
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        // Performance tuning
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        config.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
        config.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);

        // JSON deserialization settings
        config.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        config.put(JsonDeserializer.VALUE_DEFAULT_TYPE, DriverLocationEvent.class.getName());
        config.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

        return new DefaultKafkaConsumerFactory<>(
            config,
            new StringDeserializer(),
            new JsonDeserializer<>(DriverLocationEvent.class, false)
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, DriverLocationEvent> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, DriverLocationEvent> factory =
            new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory());

        // MANUAL ack mode: Application must call acknowledgment.acknowledge() to commit offset
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        // Number of concurrent consumer threads (defaults to 1)
        // When deployed with HPA (2-8 replicas), each replica runs 1 consumer thread
        factory.setConcurrency(1);

        return factory;
    }
}
