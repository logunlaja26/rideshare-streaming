package com.rideshare.tripproducer.service;

import com.rideshare.tripproducer.model.TripEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Simulates realistic trip lifecycles for a rideshare platform.
 *
 * Trip Flow:
 * 1. Match rider with nearby available driver
 * 2. Emit TRIP_STARTED event (pickup location)
 * 3. Wait for trip duration (5-30 minutes)
 * 4. Emit TRIP_ENDED event (dropoff location, distance, duration)
 * 5. Calculate fare based on distance, duration, and surge pricing
 * 6. Emit FARE_CALCULATED event
 *
 * This ties into the system:
 * - Reads driver availability from the 50 drivers in gps-producer
 * - Produces events that trip-event-service will persist to Postgres
 * - Fares can later be enriched with surge pricing from surge-pricing-engine
 */
@Service
public class TripSimulatorService {

    private static final Logger log = LoggerFactory.getLogger(TripSimulatorService.class);

    private static final String TOPIC = "trip.events";
    private static final int NUM_DRIVERS = 50;  // Must match gps-producer
    private static final int NUM_RIDERS = 100;

    // San Francisco downtown area (same as GPS producer)
    private static final double BASE_LAT = 37.7749;
    private static final double BASE_LNG = -122.4194;
    private static final double LAT_RANGE = 0.05;
    private static final double LNG_RANGE = 0.05;

    // Fare calculation constants
    private static final double BASE_FARE = 2.50;           // Base fare in dollars
    private static final double PER_KM_RATE = 1.75;         // Rate per kilometer
    private static final double PER_MINUTE_RATE = 0.35;     // Rate per minute

    private final KafkaTemplate<String, TripEvent> kafkaTemplate;
    private final Map<String, ActiveTrip> activeTrips = new ConcurrentHashMap<>();
    private final Set<String> availableDrivers = ConcurrentHashMap.newKeySet();
    private final Random random = new Random();

    @Value("${trip.simulator.enabled:true}")
    private boolean simulatorEnabled;

    @Value("${trip.simulator.num-active-trips:10}")
    private int targetActiveTrips;

    @Value("${trip.simulator.trip-duration-min:300}")
    private int tripDurationMinSeconds;

    @Value("${trip.simulator.trip-duration-max:1800}")
    private int tripDurationMaxSeconds;

    public TripSimulatorService(KafkaTemplate<String, TripEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostConstruct
    public void initialize() {
        if (!simulatorEnabled) {
            log.info("Trip simulator is disabled");
            return;
        }

        // Initialize available drivers (driver-001 through driver-050)
        for (int i = 1; i <= NUM_DRIVERS; i++) {
            availableDrivers.add(String.format("driver-%03d", i));
        }

        log.info("Trip simulator initialized with {} available drivers", availableDrivers.size());
    }

    /**
     * Main simulation loop: start new trips and end completed trips
     * Runs every 10 seconds
     */
    @Scheduled(fixedRateString = "${trip.simulator.interval-seconds:10}000", initialDelay = 5000)
    public void simulateTrips() {
        if (!simulatorEnabled) {
            return;
        }

        // End trips that have completed their duration
        endCompletedTrips();

        // Start new trips if we're below target
        int tripsToStart = targetActiveTrips - activeTrips.size();
        for (int i = 0; i < tripsToStart && !availableDrivers.isEmpty(); i++) {
            startNewTrip();
        }

        log.info("Active trips: {} | Available drivers: {}", activeTrips.size(), availableDrivers.size());
    }

    /**
     * Start a new trip by matching a rider with an available driver
     */
    private void startNewTrip() {
        // Get a random available driver
        String driverId = getRandomDriver();
        if (driverId == null) {
            log.debug("No available drivers to start new trip");
            return;
        }

        // Generate trip details
        String tripId = UUID.randomUUID().toString();
        String riderId = String.format("rider-%03d", random.nextInt(NUM_RIDERS) + 1);

        // Pickup location (random location in SF area)
        double pickupLat = BASE_LAT + (random.nextDouble() - 0.5) * LAT_RANGE;
        double pickupLng = BASE_LNG + (random.nextDouble() - 0.5) * LNG_RANGE;

        // Dropoff location (2-10 km away from pickup)
        double distanceKm = 2.0 + random.nextDouble() * 8.0;
        double angle = random.nextDouble() * 2 * Math.PI;
        double latDelta = (distanceKm / 111.0) * Math.cos(angle);  // 1 degree lat ≈ 111 km
        double lngDelta = (distanceKm / (111.0 * Math.cos(Math.toRadians(pickupLat)))) * Math.sin(angle);

        double dropoffLat = pickupLat + latDelta;
        double dropoffLng = pickupLng + lngDelta;

        // Trip duration (random between configured min/max)
        int durationSeconds = tripDurationMinSeconds +
            random.nextInt(tripDurationMaxSeconds - tripDurationMinSeconds);

        // Mark driver as unavailable
        availableDrivers.remove(driverId);

        // Store active trip
        ActiveTrip trip = new ActiveTrip(
            tripId, driverId, riderId,
            pickupLat, pickupLng,
            dropoffLat, dropoffLng,
            distanceKm, durationSeconds,
            System.currentTimeMillis()
        );
        activeTrips.put(tripId, trip);

        // Emit TRIP_STARTED event
        TripEvent event = TripEvent.tripStarted(tripId, driverId, riderId, pickupLat, pickupLng);
        sendEvent(tripId, event);

        log.info("Started trip {} with driver {} and rider {} (pickup: [{}, {}], duration: {}s)",
            tripId, driverId, riderId, pickupLat, pickupLng, durationSeconds);
    }

    /**
     * Check for completed trips and emit TRIP_ENDED and FARE_CALCULATED events
     */
    private void endCompletedTrips() {
        long now = System.currentTimeMillis();

        List<String> completedTripIds = activeTrips.entrySet().stream()
            .filter(entry -> {
                ActiveTrip trip = entry.getValue();
                long elapsedMs = now - trip.startTimeMs;
                return elapsedMs >= (trip.durationSeconds * 1000L);
            })
            .map(Map.Entry::getKey)
            .toList();

        for (String tripId : completedTripIds) {
            ActiveTrip trip = activeTrips.remove(tripId);
            if (trip != null) {
                endTrip(trip);
            }
        }
    }

    /**
     * End a trip: emit TRIP_ENDED and FARE_CALCULATED events
     */
    private void endTrip(ActiveTrip trip) {
        // Emit TRIP_ENDED event
        TripEvent endEvent = TripEvent.tripEnded(
            trip.tripId,
            trip.dropoffLat, trip.dropoffLng,
            trip.distanceKm,
            trip.durationSeconds
        );
        sendEvent(trip.tripId, endEvent);

        // Calculate fare
        double durationMinutes = trip.durationSeconds / 60.0;
        double fare = BASE_FARE +
                     (trip.distanceKm * PER_KM_RATE) +
                     (durationMinutes * PER_MINUTE_RATE);

        // Round to 2 decimal places
        fare = Math.round(fare * 100.0) / 100.0;

        // Emit FARE_CALCULATED event
        TripEvent fareEvent = TripEvent.fareCalculated(trip.tripId, fare);
        sendEvent(trip.tripId, fareEvent);

        // Make driver available again
        availableDrivers.add(trip.driverId);

        log.info("Ended trip {} - Distance: {:.2f}km, Duration: {}s, Fare: ${:.2f}",
            trip.tripId, trip.distanceKm, trip.durationSeconds, fare);
    }

    /**
     * Send event to Kafka, keyed by tripId for ordering guarantee
     */
    private void sendEvent(String tripId, TripEvent event) {
        kafkaTemplate.send(TOPIC, tripId, event)
            .whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to send {} event for trip {}: {}",
                        event.eventType(), tripId, ex.getMessage());
                } else {
                    log.debug("Sent {} event for trip {} to partition {}",
                        event.eventType(), tripId,
                        result.getRecordMetadata().partition());
                }
            });
    }

    /**
     * Get a random available driver
     */
    private String getRandomDriver() {
        if (availableDrivers.isEmpty()) {
            return null;
        }
        List<String> drivers = new ArrayList<>(availableDrivers);
        return drivers.get(random.nextInt(drivers.size()));
    }

    /**
     * Internal state for an active trip
     */
    private static class ActiveTrip {
        final String tripId;
        final String driverId;
        final String riderId;
        final double pickupLat;
        final double pickupLng;
        final double dropoffLat;
        final double dropoffLng;
        final double distanceKm;
        final int durationSeconds;
        final long startTimeMs;

        ActiveTrip(String tripId, String driverId, String riderId,
                  double pickupLat, double pickupLng,
                  double dropoffLat, double dropoffLng,
                  double distanceKm, int durationSeconds,
                  long startTimeMs) {
            this.tripId = tripId;
            this.driverId = driverId;
            this.riderId = riderId;
            this.pickupLat = pickupLat;
            this.pickupLng = pickupLng;
            this.dropoffLat = dropoffLat;
            this.dropoffLng = dropoffLng;
            this.distanceKm = distanceKm;
            this.durationSeconds = durationSeconds;
            this.startTimeMs = startTimeMs;
        }
    }
}
