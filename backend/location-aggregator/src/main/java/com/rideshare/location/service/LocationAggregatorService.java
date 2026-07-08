package com.rideshare.location.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rideshare.location.model.DriverLocationEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

/**
 * Kafka consumer that reads driver location events and writes them to Redis.
 *
 * Design decisions:
 * - Manual offset commit (enable-auto-commit=false) ensures Redis write happens before committing offset
 * - Redis HASH structure: key = "driver:locations", field = driverId, value = DriverLocationEvent JSON
 * - Consumer group allows horizontal scaling (max 8 instances = 8 partitions)
 * - Partition assignment logged for observability
 */
@Service
public class LocationAggregatorService {

    private static final Logger log = LoggerFactory.getLogger(LocationAggregatorService.class);
    private static final String DRIVER_LOCATIONS_KEY = "driver:locations";

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    public LocationAggregatorService(StringRedisTemplate redisTemplate, ObjectMapper objectMapper) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * Consumes driver location events from Kafka and stores them in Redis.
     *
     * @param event The driver location event
     * @param partition The partition this message came from
     * @param offset The offset of this message
     * @param acknowledgment Manual acknowledgment for offset commit
     */
    @KafkaListener(
        topics = "driver.location",
        groupId = "location-aggregator-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeDriverLocation(
        @Payload DriverLocationEvent event,
        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
        @Header(KafkaHeaders.OFFSET) long offset,
        Acknowledgment acknowledgment
    ) {
        try {
            log.debug("Received location for {} from partition {} offset {} - lat: {}, lng: {}, speed: {} km/h",
                event.driverId(), partition, offset, event.lat(), event.lng(), event.speed());

            // Write to Redis HASH: HSET driver:locations {driverId} {JSON}
            // Manually serialize to JSON string for reliability
            String eventJson = objectMapper.writeValueAsString(event);
            redisTemplate.opsForHash().put(DRIVER_LOCATIONS_KEY, event.driverId(), eventJson);

            // Manually commit offset only after successful Redis write
            // This prevents data loss if consumer crashes before persisting to Redis
            acknowledgment.acknowledge();

            log.debug("Stored location for {} in Redis and committed offset {}", event.driverId(), offset);

        } catch (Exception e) {
            log.error("Error processing location event for driver {} from partition {} offset {}: {}",
                event.driverId(), partition, offset, e.getMessage(), e);
            // Do NOT acknowledge - this message will be redelivered
            // In Phase 3 Step 5, we'll add DLQ handling for persistent failures
            throw new RuntimeException("Failed to process location event for " + event.driverId(), e);
        }
    }

    /**
     * Returns the total number of drivers currently tracked in Redis.
     * Useful for monitoring and debugging.
     */
    public long getTrackedDriverCount() {
        Long size = redisTemplate.opsForHash().size(DRIVER_LOCATIONS_KEY);
        return size != null ? size : 0;
    }
}
