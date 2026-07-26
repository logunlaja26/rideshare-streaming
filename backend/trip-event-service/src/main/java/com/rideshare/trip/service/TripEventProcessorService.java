package com.rideshare.trip.service;

import com.rideshare.trip.entity.Fare;
import com.rideshare.trip.entity.Trip;
import com.rideshare.trip.model.TripEvent;
import com.rideshare.trip.model.TripEventType;
import com.rideshare.trip.repository.FareRepository;
import com.rideshare.trip.repository.TripRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Kafka consumer that processes trip lifecycle events.
 * Uses Java 21 switch expressions for clean event routing.
 *
 * Phase 3 Step 5: Persists trip data to Postgres via JPA
 * Phase 4: Will add exactly-once semantics with transactional.id
 */
@Service
public class TripEventProcessorService {

    private static final Logger log = LoggerFactory.getLogger(TripEventProcessorService.class);

    private final TripRepository tripRepository;
    private final FareRepository fareRepository;

    public TripEventProcessorService(TripRepository tripRepository, FareRepository fareRepository) {
        this.tripRepository = tripRepository;
        this.fareRepository = fareRepository;
    }

    /**
     * Consumes trip events from Kafka and routes them based on event type.
     * Uses switch expressions (Java 21) for type-safe, exhaustive routing.
     *
     * @param event The trip event
     * @param partition The partition this message came from
     * @param offset The offset of this message
     */
    @KafkaListener(
        topics = "trip.events",
        groupId = "trip-event-service-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeTripEvent(
        @Payload TripEvent event,
        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
        @Header(KafkaHeaders.OFFSET) long offset
    ) {
        try {
            log.debug("Received {} event for trip {} from partition {} offset {}",
                event.eventType(), event.tripId(), partition, offset);

            // Route event based on type using switch expression
            String result = switch (event.eventType()) {
                case TRIP_STARTED -> handleTripStarted(event);
                case TRIP_ENDED -> handleTripEnded(event);
                case FARE_CALCULATED -> handleFareCalculated(event);
            };

            log.debug("{}", result);

        } catch (Exception e) {
            log.error("Error processing {} event for trip {} from partition {} offset {}: {}",
                event.eventType(), event.tripId(), partition, offset, e.getMessage(), e);
            // In Phase 3 Step 5, we'll add DLQ handling for persistent failures
            throw new RuntimeException("Failed to process trip event for " + event.tripId(), e);
        }
    }

    /**
     * Handles TRIP_STARTED events.
     * Creates a new Trip entity in Postgres with pickup location and start time.
     */
    private String handleTripStarted(TripEvent event) {
        log.info("TRIP_STARTED: tripId={}, driver={}, rider={}, pickup=[{}, {}]",
            event.tripId(), event.driverId(), event.riderId(),
            event.pickupLat(), event.pickupLng());

        // Create Trip entity
        Trip trip = new Trip(event.tripId(), event.driverId(), event.riderId());
        trip.setPickupLat(event.pickupLat());
        trip.setPickupLng(event.pickupLng());
        trip.setStartedAt(Instant.ofEpochMilli(event.timestamp()));

        tripRepository.save(trip);
        log.debug("Persisted trip {} to Postgres", event.tripId());

        return String.format("Trip %s started successfully", event.tripId());
    }

    /**
     * Handles TRIP_ENDED events.
     * Updates the Trip entity with completion data (dropoff location, distance, duration).
     */
    private String handleTripEnded(TripEvent event) {
        log.info("TRIP_ENDED: tripId={}, dropoff=[{}, {}], distance={}km, duration={}s",
            event.tripId(), event.dropoffLat(), event.dropoffLng(),
            event.distance(), event.duration());

        // Update Trip entity with completion data
        Trip trip = tripRepository.findByTripId(event.tripId())
            .orElseThrow(() -> new IllegalStateException(
                "Trip " + event.tripId() + " not found - TRIP_STARTED event may have been lost"));

        trip.setDropoffLat(event.dropoffLat());
        trip.setDropoffLng(event.dropoffLng());
        trip.setDistanceKm(event.distance());
        trip.setDurationSeconds(event.duration());
        trip.setEndedAt(Instant.ofEpochMilli(event.timestamp()));

        tripRepository.save(trip);
        log.debug("Updated trip {} with completion data in Postgres", event.tripId());

        return String.format("Trip %s ended - %s km in %s seconds",
            event.tripId(), event.distance(), event.duration());
    }

    /**
     * Handles FARE_CALCULATED events.
     * Creates a Fare entity in Postgres with the calculated fare amount.
     */
    private String handleFareCalculated(TripEvent event) {
        log.info("FARE_CALCULATED: tripId={}, fare=${:.2f}",
            event.tripId(), event.fareAmount());

        // Create Fare entity
        Fare fare = new Fare(
            event.tripId(),
            BigDecimal.valueOf(event.fareAmount()),
            Instant.ofEpochMilli(event.timestamp())
        );

        fareRepository.save(fare);
        log.debug("Persisted fare for trip {} to Postgres: ${}", event.tripId(), event.fareAmount());

        return String.format("Fare calculated for trip %s: $%.2f",
            event.tripId(), event.fareAmount());
    }
}
