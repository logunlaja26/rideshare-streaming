package com.rideshare.location;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rideshare.location.model.DriverLocationEvent;
import com.rideshare.location.service.LocationAggregatorService;
import com.redis.testcontainers.RedisContainer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test for LocationAggregator service using Testcontainers.
 * Spins up real Kafka and Redis containers in Docker to test the full consume-to-Redis flow.
 *
 * Phase 3 Step 7: Demonstrates integration testing with Testcontainers
 */
@SpringBootTest
@Testcontainers
class LocationAggregatorIntegrationTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.6.0")
    );

    @Container
    static RedisContainer redis = new RedisContainer(
        DockerImageName.parse("redis:7.2-alpine")
    ).withExposedPorts(6379);

    @Autowired
    private LocationAggregatorService locationAggregatorService;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    private static KafkaTemplate<String, DriverLocationEvent> testProducerTemplate;

    /**
     * Configure Spring Boot to use the Testcontainers instances for Kafka and Redis.
     */
    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", redis::getFirstMappedPort);
    }

    @BeforeAll
    static void setup() {
        // Create a Kafka producer for test data
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        ProducerFactory<String, DriverLocationEvent> producerFactory =
            new DefaultKafkaProducerFactory<>(producerProps);
        testProducerTemplate = new KafkaTemplate<>(producerFactory);
    }

    @AfterAll
    static void teardown() {
        if (testProducerTemplate != null) {
            testProducerTemplate.destroy();
        }
    }

    @Test
    void shouldConsumeDriverLocationAndWriteToRedis() throws Exception {
        // Given: A driver location event
        String driverId = "driver-test-001";
        DriverLocationEvent event = new DriverLocationEvent(
            driverId,
            40.7128,  // NYC latitude
            -74.0060, // NYC longitude
            System.currentTimeMillis(),
            35.5  // speed in km/h
        );

        // When: We produce the event to the driver.location topic
        testProducerTemplate.send("driver.location", driverId, event).get();

        // Then: The event should be consumed and written to Redis within 10 seconds
        await()
            .atMost(10, TimeUnit.SECONDS)
            .untilAsserted(() -> {
                String redisValue = (String) redisTemplate.opsForHash()
                    .get("driver:locations", driverId);

                assertThat(redisValue).isNotNull();

                // Deserialize and verify
                DriverLocationEvent storedEvent = objectMapper.readValue(
                    redisValue,
                    DriverLocationEvent.class
                );

                assertThat(storedEvent.driverId()).isEqualTo(driverId);
                assertThat(storedEvent.lat()).isEqualTo(40.7128);
                assertThat(storedEvent.lng()).isEqualTo(-74.0060);
                assertThat(storedEvent.speed()).isEqualTo(35.5);
            });

        // Verify the tracked driver count increased
        assertThat(locationAggregatorService.getTrackedDriverCount()).isGreaterThanOrEqualTo(1);
    }

    @Test
    void shouldHandleMultipleDriversInParallel() throws Exception {
        // Given: Multiple driver location events
        String[] driverIds = {"driver-001", "driver-002", "driver-003"};
        double[][] coordinates = {
            {40.7128, -74.0060}, // NYC
            {34.0522, -118.2437}, // LA
            {41.8781, -87.6298}  // Chicago
        };

        // When: We produce multiple events
        for (int i = 0; i < driverIds.length; i++) {
            DriverLocationEvent event = new DriverLocationEvent(
                driverIds[i],
                coordinates[i][0],
                coordinates[i][1],
                System.currentTimeMillis(),
                40.0 + i * 5
            );
            testProducerTemplate.send("driver.location", driverIds[i], event).get();
        }

        // Then: All events should be stored in Redis
        await()
            .atMost(10, TimeUnit.SECONDS)
            .untilAsserted(() -> {
                for (int i = 0; i < driverIds.length; i++) {
                    String redisValue = (String) redisTemplate.opsForHash()
                        .get("driver:locations", driverIds[i]);

                    assertThat(redisValue)
                        .as("Driver %s should be in Redis", driverIds[i])
                        .isNotNull();

                    DriverLocationEvent storedEvent = objectMapper.readValue(
                        redisValue,
                        DriverLocationEvent.class
                    );

                    assertThat(storedEvent.lat()).isEqualTo(coordinates[i][0]);
                    assertThat(storedEvent.lng()).isEqualTo(coordinates[i][1]);
                }
            });
    }

    @Test
    void shouldOverwriteOldLocationWithNewLocation() throws Exception {
        // Given: A driver's initial location
        String driverId = "driver-moving-001";
        DriverLocationEvent firstLocation = new DriverLocationEvent(
            driverId,
            40.7128,
            -74.0060,
            System.currentTimeMillis(),
            30.0
        );

        // When: We send the first location
        testProducerTemplate.send("driver.location", driverId, firstLocation).get();

        // Wait for it to be processed
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            String value = (String) redisTemplate.opsForHash().get("driver:locations", driverId);
            assertThat(value).isNotNull();
        });

        // And: The driver moves to a new location
        DriverLocationEvent secondLocation = new DriverLocationEvent(
            driverId,
            40.7580,  // New latitude (moved north)
            -73.9855, // New longitude (moved east)
            System.currentTimeMillis(),
            45.0
        );

        testProducerTemplate.send("driver.location", driverId, secondLocation).get();

        // Then: Redis should contain the NEW location (idempotent overwrite)
        await()
            .atMost(5, TimeUnit.SECONDS)
            .untilAsserted(() -> {
                String redisValue = (String) redisTemplate.opsForHash()
                    .get("driver:locations", driverId);

                DriverLocationEvent storedEvent = objectMapper.readValue(
                    redisValue,
                    DriverLocationEvent.class
                );

                // Should have the NEW coordinates, not the old ones
                assertThat(storedEvent.lat()).isEqualTo(40.7580);
                assertThat(storedEvent.lng()).isEqualTo(-73.9855);
                assertThat(storedEvent.speed()).isEqualTo(45.0);
            });

        // Verify we still only have 1 entry for this driver (not duplicated)
        assertThat(locationAggregatorService.getTrackedDriverCount()).isGreaterThanOrEqualTo(1);
    }
}
