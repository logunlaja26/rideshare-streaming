package com.rideshare.trip.service;

import com.rideshare.trip.model.TripEvent;
import com.rideshare.trip.model.TripEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

/**
 * Kafka consumer that processes trip lifecycle events.
 * Uses Java 21 switch expressions for clean event routing.
 *
 * Phase 3: Logs events (validation of event flow)
 * Phase 4: Will add JPA persistence and exactly-once semantics
 */
@Service
public class TripEventProcessorService {

    private static final Logger log = LoggerFactory.getLogger(TripEventProcessorService.class);

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
     * In Phase 4, will create Trip entity in Postgres.
     */
    private String handleTripStarted(TripEvent event) {
        log.info("TRIP_STARTED: tripId={}, driver={}, rider={}, pickup=[{}, {}]",
            event.tripId(), event.driverId(), event.riderId(),
            event.pickupLat(), event.pickupLng());

        // Phase 4: Create Trip entity
        // tripRepository.save(new Trip(event.tripId(), event.driverId(), event.riderId(), ...));

        return String.format("Trip %s started successfully", event.tripId());
    }

    /**
     * Handles TRIP_ENDED events.
     * In Phase 4, will update Trip entity with completion data.
     */
    private String handleTripEnded(TripEvent event) {
        log.info("TRIP_ENDED: tripId={}, dropoff=[{}, {}], distance={}km, duration={}s",
            event.tripId(), event.dropoffLat(), event.dropoffLng(),
            event.distance(), event.duration());

        // Phase 4: Update Trip entity
        // Trip trip = tripRepository.findById(event.tripId()).orElseThrow();
        // trip.setDropoffLat(...); trip.setDistance(...); ...
        // tripRepository.save(trip);

        return String.format("Trip %s ended - %s km in %s seconds",
            event.tripId(), event.distance(), event.duration());
    }

    /**
     * Handles FARE_CALCULATED events.
     * In Phase 4, will create Fare entity in Postgres.
     */
    private String handleFareCalculated(TripEvent event) {
        log.info("FARE_CALCULATED: tripId={}, fare=${:.2f}",
            event.tripId(), event.fareAmount());

        // Phase 4: Create Fare entity
        // fareRepository.save(new Fare(event.tripId(), event.fareAmount(), ...));

        return String.format("Fare calculated for trip %s: $%.2f",
            event.tripId(), event.fareAmount());
    }
}
