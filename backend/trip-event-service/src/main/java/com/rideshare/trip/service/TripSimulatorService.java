package com.rideshare.trip.service;

import com.rideshare.trip.model.TripEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simulates the rider/driver trip lifecycle and produces TRIP_STARTED, TRIP_ENDED and
 * FARE_CALCULATED events to the trip.events Kafka topic, keyed by tripId.
 *
 * Each tick starts a handful of new trips and completes any trips whose simulated duration
 * has elapsed, emitting TRIP_ENDED immediately followed by FARE_CALCULATED for that trip.
 */
@Service
public class TripSimulatorService {

    private static final Logger log = LoggerFactory.getLogger(TripSimulatorService.class);

    private static final String TOPIC = "trip.events";
    private static final int NUM_DRIVERS = 50;
    private static final int NUM_RIDERS = 30;
    private static final int MAX_CONCURRENT_TRIPS = 15;

    // San Francisco downtown area boundaries (matches gps-producer)
    private static final double BASE_LAT = 37.7749;
    private static final double BASE_LNG = -122.4194;
    private static final double LAT_RANGE = 0.05;
    private static final double LNG_RANGE = 0.05;

    private static final double BASE_FARE = 2.50;
    private static final double PER_KM_RATE = 1.75;

    private final KafkaTemplate<String, TripEvent> kafkaTemplate;
    private final Map<String, ActiveTrip> activeTrips = new ConcurrentHashMap<>();
    private final Random random = new Random();
    private final AtomicLong tripSequence = new AtomicLong();

    @Value("${trip.simulator.enabled:true}")
    private boolean simulatorEnabled;

    @Value("${trip.simulator.min-duration-seconds:15}")
    private int minTripSeconds;

    @Value("${trip.simulator.max-duration-seconds:45}")
    private int maxTripSeconds;

    public TripSimulatorService(KafkaTemplate<String, TripEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Every 5 seconds, complete any trips that are due and start a few new ones.
     */
    @Scheduled(fixedRate = 5000, initialDelay = 5000)
    public void tick() {
        if (!simulatorEnabled) {
            return;
        }
        completeDueTrips();
        startNewTrips();
    }

    private void completeDueTrips() {
        long now = System.currentTimeMillis();
        Iterator<Map.Entry<String, ActiveTrip>> it = activeTrips.entrySet().iterator();

        while (it.hasNext()) {
            ActiveTrip trip = it.next().getValue();

            if (trip.endAtMillis <= now) {
                send(TripEvent.ended(trip.tripId, trip.driverId, trip.riderId,
                    trip.originLat, trip.originLng, trip.destLat, trip.destLng));

                double fare = calculateFare(trip);
                send(TripEvent.fareCalculated(trip.tripId, trip.driverId, trip.riderId,
                    trip.originLat, trip.originLng, trip.destLat, trip.destLng, fare));

                it.remove();
            }
        }
    }

    private void startNewTrips() {
        if (activeTrips.size() >= MAX_CONCURRENT_TRIPS) {
            return;
        }

        int tripsToStart = 1 + random.nextInt(3);
        for (int i = 0; i < tripsToStart && activeTrips.size() < MAX_CONCURRENT_TRIPS; i++) {
            String tripId = "trip-" + tripSequence.incrementAndGet();
            String driverId = String.format("driver-%03d", 1 + random.nextInt(NUM_DRIVERS));
            String riderId = String.format("rider-%03d", 1 + random.nextInt(NUM_RIDERS));

            double originLat = BASE_LAT + (random.nextDouble() - 0.5) * LAT_RANGE;
            double originLng = BASE_LNG + (random.nextDouble() - 0.5) * LNG_RANGE;
            double destLat = BASE_LAT + (random.nextDouble() - 0.5) * LAT_RANGE;
            double destLng = BASE_LNG + (random.nextDouble() - 0.5) * LNG_RANGE;

            int durationSeconds = minTripSeconds >= maxTripSeconds
                ? minTripSeconds
                : minTripSeconds + random.nextInt(maxTripSeconds - minTripSeconds + 1);
            long endAtMillis = System.currentTimeMillis() + durationSeconds * 1000L;

            ActiveTrip trip = new ActiveTrip(tripId, driverId, riderId,
                originLat, originLng, destLat, destLng, endAtMillis);
            activeTrips.put(tripId, trip);

            send(TripEvent.started(tripId, driverId, riderId, originLat, originLng, destLat, destLng));
        }
    }

    private double calculateFare(ActiveTrip trip) {
        double distanceKm = haversineKm(trip.originLat, trip.originLng, trip.destLat, trip.destLng);
        double surgeMultiplier = 1.0 + random.nextDouble() * 0.5;
        double fare = (BASE_FARE + distanceKm * PER_KM_RATE) * surgeMultiplier;
        return Math.round(fare * 100.0) / 100.0;
    }

    private double haversineKm(double lat1, double lng1, double lat2, double lng2) {
        double earthRadiusKm = 6371.0;
        double dLat = Math.toRadians(lat2 - lat1);
        double dLng = Math.toRadians(lng2 - lng1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
            + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
            * Math.sin(dLng / 2) * Math.sin(dLng / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return earthRadiusKm * c;
    }

    private void send(TripEvent event) {
        // Key by tripId to guarantee ordering per trip
        kafkaTemplate.send(TOPIC, event.tripId(), event)
            .whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to send {} for {}: {}", event.eventType(), event.tripId(), ex.getMessage());
                } else {
                    log.debug("Sent {} for {} to partition {}",
                        event.eventType(), event.tripId(), result.getRecordMetadata().partition());
                }
            });
    }

    private static class ActiveTrip {
        final String tripId;
        final String driverId;
        final String riderId;
        final double originLat;
        final double originLng;
        final double destLat;
        final double destLng;
        final long endAtMillis;

        ActiveTrip(String tripId, String driverId, String riderId,
                   double originLat, double originLng, double destLat, double destLng,
                   long endAtMillis) {
            this.tripId = tripId;
            this.driverId = driverId;
            this.riderId = riderId;
            this.originLat = originLat;
            this.originLng = originLng;
            this.destLat = destLat;
            this.destLng = destLng;
            this.endAtMillis = endAtMillis;
        }
    }
}
