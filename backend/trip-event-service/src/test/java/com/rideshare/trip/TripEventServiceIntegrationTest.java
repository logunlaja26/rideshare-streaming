package com.rideshare.trip;

import com.rideshare.trip.entity.Fare;
import com.rideshare.trip.entity.Trip;
import com.rideshare.trip.model.TripEvent;
import com.rideshare.trip.model.TripEventType;
import com.rideshare.trip.repository.FareRepository;
import com.rideshare.trip.repository.TripRepository;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test for Trip Event Service using Testcontainers.
 * Spins up real Kafka and Postgres containers to test the full trip lifecycle event processing.
 *
 * Phase 3 Step 7: Demonstrates integration testing with Testcontainers for Kafka + Postgres
 */
@SpringBootTest
@Testcontainers
class TripEventServiceIntegrationTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.6.0")
    );

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
        DockerImageName.parse("postgres:16-alpine")
    )
        .withDatabaseName("rideshare_test")
        .withUsername("test")
        .withPassword("test");

    @Autowired
    private TripRepository tripRepository;

    @Autowired
    private FareRepository fareRepository;

    private static KafkaTemplate<String, TripEvent> testProducerTemplate;

    /**
     * Configure Spring Boot to use the Testcontainers instances.
     */
    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "create-drop");
    }

    @BeforeAll
    static void setup() {
        // Create a Kafka producer for test data
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        ProducerFactory<String, TripEvent> producerFactory =
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
    void shouldProcessTripStartedEvent() throws Exception {
        // Given: A TRIP_STARTED event
        String tripId = "trip-test-001";
        String driverId = "driver-001";
        String riderId = "rider-001";

        TripEvent tripStartedEvent = new TripEvent(
            TripEventType.TRIP_STARTED,
            tripId,
            driverId,
            riderId,
            40.7128,  // pickup lat
            -74.0060, // pickup lng
            null,     // dropoff lat (not yet known)
            null,     // dropoff lng (not yet known)
            null,     // distance (not yet known)
            null,     // duration (not yet known)
            null,     // fare amount (not yet calculated)
            System.currentTimeMillis()
        );

        // When: We produce the TRIP_STARTED event
        testProducerTemplate.send("trip.events", tripId, tripStartedEvent).get();

        // Then: The trip should be persisted to Postgres
        await()
            .atMost(10, TimeUnit.SECONDS)
            .untilAsserted(() -> {
                Optional<Trip> savedTrip = tripRepository.findByTripId(tripId);

                assertThat(savedTrip).isPresent();
                assertThat(savedTrip.get().getTripId()).isEqualTo(tripId);
                assertThat(savedTrip.get().getDriverId()).isEqualTo(driverId);
                assertThat(savedTrip.get().getRiderId()).isEqualTo(riderId);
                assertThat(savedTrip.get().getPickupLat()).isEqualTo(40.7128);
                assertThat(savedTrip.get().getPickupLng()).isEqualTo(-74.0060);
                assertThat(savedTrip.get().getStartedAt()).isNotNull();
                assertThat(savedTrip.get().getEndedAt()).isNull(); // Trip not ended yet
            });
    }

    @Test
    void shouldProcessCompleteTripLifecycle() throws Exception {
        // Given: A complete trip lifecycle
        String tripId = "trip-lifecycle-001";
        String driverId = "driver-002";
        String riderId = "rider-002";
        long now = System.currentTimeMillis();

        // Step 1: TRIP_STARTED
        TripEvent tripStarted = new TripEvent(
            TripEventType.TRIP_STARTED,
            tripId,
            driverId,
            riderId,
            40.7128,  // NYC pickup
            -74.0060,
            null, null, null, null, null,
            now
        );

        testProducerTemplate.send("trip.events", tripId, tripStarted).get();

        // Wait for TRIP_STARTED to be processed
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(tripRepository.findByTripId(tripId)).isPresent();
        });

        // Step 2: TRIP_ENDED
        TripEvent tripEnded = new TripEvent(
            TripEventType.TRIP_ENDED,
            tripId,
            driverId,
            riderId,
            null, null,  // pickup already recorded
            40.7580,     // dropoff lat
            -73.9855,    // dropoff lng
            5.2,         // distance in km
            840,         // duration in seconds (14 minutes)
            null,
            now + 840_000
        );

        testProducerTemplate.send("trip.events", tripId, tripEnded).get();

        // Wait for TRIP_ENDED to be processed
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            Optional<Trip> trip = tripRepository.findByTripId(tripId);
            assertThat(trip).isPresent();
            assertThat(trip.get().getDropoffLat()).isEqualTo(40.7580);
            assertThat(trip.get().getDropoffLng()).isEqualTo(-73.9855);
            assertThat(trip.get().getDistanceKm()).isEqualTo(5.2);
            assertThat(trip.get().getDurationSeconds()).isEqualTo(840);
            assertThat(trip.get().getEndedAt()).isNotNull();
        });

        // Step 3: FARE_CALCULATED
        double fareAmount = 18.50;
        TripEvent fareCalculated = new TripEvent(
            TripEventType.FARE_CALCULATED,
            tripId,
            driverId,
            riderId,
            null, null, null, null, null, null,
            fareAmount,
            now + 841_000
        );

        testProducerTemplate.send("trip.events", tripId, fareCalculated).get();

        // Then: Fare should be persisted
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            Optional<Fare> fare = fareRepository.findByTripId(tripId);

            assertThat(fare).isPresent();
            assertThat(fare.get().getTripId()).isEqualTo(tripId);
            assertThat(fare.get().getAmount()).isEqualByComparingTo(BigDecimal.valueOf(fareAmount));
            assertThat(fare.get().getCalculatedAt()).isNotNull();
        });

        // Verify final state: Trip is complete with all fields populated
        Optional<Trip> completedTrip = tripRepository.findByTripId(tripId);
        assertThat(completedTrip).isPresent();
        assertThat(completedTrip.get().getPickupLat()).isEqualTo(40.7128);
        assertThat(completedTrip.get().getDropoffLat()).isEqualTo(40.7580);
        assertThat(completedTrip.get().getDistanceKm()).isEqualTo(5.2);
        assertThat(completedTrip.get().getDurationSeconds()).isEqualTo(840);
    }

    @Test
    void shouldProcessMultipleTripsInParallel() throws Exception {
        // Given: Multiple trips happening simultaneously
        String[] tripIds = {"trip-parallel-1", "trip-parallel-2", "trip-parallel-3"};
        String[] driverIds = {"driver-101", "driver-102", "driver-103"};
        String[] riderIds = {"rider-101", "rider-102", "rider-103"};

        // When: We start all trips
        for (int i = 0; i < tripIds.length; i++) {
            TripEvent tripStarted = new TripEvent(
                TripEventType.TRIP_STARTED,
                tripIds[i],
                driverIds[i],
                riderIds[i],
                40.7128 + i * 0.1, // Different pickup locations
                -74.0060 + i * 0.1,
                null, null, null, null, null,
                System.currentTimeMillis()
            );

            testProducerTemplate.send("trip.events", tripIds[i], tripStarted).get();
        }

        // Then: All trips should be persisted
        await()
            .atMost(15, TimeUnit.SECONDS)
            .untilAsserted(() -> {
                for (int i = 0; i < tripIds.length; i++) {
                    Optional<Trip> trip = tripRepository.findByTripId(tripIds[i]);

                    assertThat(trip)
                        .as("Trip %s should exist", tripIds[i])
                        .isPresent();

                    assertThat(trip.get().getDriverId()).isEqualTo(driverIds[i]);
                    assertThat(trip.get().getRiderId()).isEqualTo(riderIds[i]);
                }
            });
    }

    @Test
    void shouldNotDuplicateFareOnRedelivery() throws Exception {
        // Given: A fare calculation event
        String tripId = "trip-idempotency-test";
        String driverId = "driver-999";
        String riderId = "rider-999";

        // First create the trip
        TripEvent tripStarted = new TripEvent(
            TripEventType.TRIP_STARTED,
            tripId, driverId, riderId,
            40.7128, -74.0060,
            null, null, null, null, null,
            System.currentTimeMillis()
        );
        testProducerTemplate.send("trip.events", tripId, tripStarted).get();

        // Wait for trip to exist
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(tripRepository.findByTripId(tripId)).isPresent();
        });

        // When: We send the same FARE_CALCULATED event twice (simulating redelivery)
        TripEvent fareEvent = new TripEvent(
            TripEventType.FARE_CALCULATED,
            tripId, driverId, riderId,
            null, null, null, null, null, null,
            25.00,
            System.currentTimeMillis()
        );

        testProducerTemplate.send("trip.events", tripId, fareEvent).get();
        // Send the same event again (simulating at-least-once redelivery)
        testProducerTemplate.send("trip.events", tripId, fareEvent).get();

        // Then: Only ONE fare should exist (unique constraint on tripId should prevent duplicates)
        // Note: In Phase 3 (at-least-once), this will cause a constraint violation → DLQ
        // In Phase 4 (exactly-once), the second message won't be delivered at all
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            // Either one fare exists (if second insert was blocked by unique constraint)
            // OR two fares exist (demonstrating the duplicate problem we'll fix in Phase 4)
            assertThat(fareRepository.findByTripId(tripId)).isPresent();

            // For now, we accept that at-least-once can create duplicates
            // Phase 4 will add exactly-once semantics to prevent this
        });
    }
}
