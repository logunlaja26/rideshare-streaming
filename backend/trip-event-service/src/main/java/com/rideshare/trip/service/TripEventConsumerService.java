package com.rideshare.trip.service;

import com.rideshare.trip.entity.Fare;
import com.rideshare.trip.entity.Trip;
import com.rideshare.trip.model.TripEvent;
import com.rideshare.trip.repository.FareRepository;
import com.rideshare.trip.repository.TripRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Consumes trip.events and persists trip lifecycle state to Postgres.
 * Routes each event to a handler based on eventType.
 */
@Service
public class TripEventConsumerService {

    private static final Logger log = LoggerFactory.getLogger(TripEventConsumerService.class);

    private final TripRepository tripRepository;
    private final FareRepository fareRepository;

    public TripEventConsumerService(TripRepository tripRepository, FareRepository fareRepository) {
        this.tripRepository = tripRepository;
        this.fareRepository = fareRepository;
    }

    @KafkaListener(topics = "trip.events", groupId = "trip-event-service-group")
    @Transactional
    public void consumeTripEvent(TripEvent event) {
        log.debug("Received {} for trip {}", event.eventType(), event.tripId());

        switch (event.eventType()) {
            case TRIP_STARTED -> handleTripStarted(event);
            case TRIP_ENDED -> handleTripEnded(event);
            case FARE_CALCULATED -> handleFareCalculated(event);
        }
    }

    private void handleTripStarted(TripEvent event) {
        Trip trip = new Trip(event.tripId(), event.driverId(), event.riderId(),
            event.originLat(), event.originLng(), event.destLat(), event.destLng());
        tripRepository.save(trip);
        log.info("Trip {} started: driver={}, rider={}", event.tripId(), event.driverId(), event.riderId());
    }

    private void handleTripEnded(TripEvent event) {
        tripRepository.findById(event.tripId()).ifPresentOrElse(
            trip -> {
                trip.markEnded();
                tripRepository.save(trip);
                log.info("Trip {} ended", event.tripId());
            },
            () -> log.warn("Received TRIP_ENDED for unknown trip {}", event.tripId())
        );
    }

    private void handleFareCalculated(TripEvent event) {
        if (event.fareAmount() == null) {
            log.warn("Received FARE_CALCULATED for trip {} with no fare amount", event.tripId());
            return;
        }
        fareRepository.save(new Fare(event.tripId(), event.fareAmount()));
        log.info("Fare calculated for trip {}: ${}", event.tripId(), event.fareAmount());
    }
}
