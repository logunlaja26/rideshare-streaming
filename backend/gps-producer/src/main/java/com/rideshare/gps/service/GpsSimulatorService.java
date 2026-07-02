package com.rideshare.gps.service;

import com.rideshare.gps.model.DriverLocationEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * GPS simulator that generates realistic driver location updates using a random-walk algorithm.
 * Emits location events to Kafka topic 'driver.location' every 2 seconds.
 */
@Service
public class GpsSimulatorService {

    private static final Logger log = LoggerFactory.getLogger(GpsSimulatorService.class);

    private static final String TOPIC = "driver.location";
    private static final int NUM_DRIVERS = 50;

    // San Francisco downtown area boundaries
    private static final double BASE_LAT = 37.7749;
    private static final double BASE_LNG = -122.4194;
    private static final double LAT_RANGE = 0.05;  // ~5.5 km
    private static final double LNG_RANGE = 0.05;

    // Random walk parameters
    private static final double MAX_STEP = 0.001;  // ~110 meters per step
    private static final double MIN_SPEED = 0.0;    // km/h
    private static final double MAX_SPEED = 80.0;   // km/h

    private final KafkaTemplate<String, DriverLocationEvent> kafkaTemplate;
    private final Map<String, DriverState> driverStates = new HashMap<>();
    private final Random random = new Random();

    @Value("${gps.simulator.enabled:true}")
    private boolean simulatorEnabled;

    public GpsSimulatorService(KafkaTemplate<String, DriverLocationEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostConstruct
    public void initializeDrivers() {
        if (!simulatorEnabled) {
            log.info("GPS simulator is disabled");
            return;
        }

        log.info("Initializing {} drivers with random positions in San Francisco area", NUM_DRIVERS);

        for (int i = 1; i <= NUM_DRIVERS; i++) {
            String driverId = String.format("driver-%03d", i);
            double lat = BASE_LAT + (random.nextDouble() - 0.5) * LAT_RANGE;
            double lng = BASE_LNG + (random.nextDouble() - 0.5) * LNG_RANGE;
            double speed = MIN_SPEED + random.nextDouble() * (MAX_SPEED - MIN_SPEED);

            driverStates.put(driverId, new DriverState(lat, lng, speed));
        }

        log.info("GPS simulator initialized with {} drivers", driverStates.size());
    }

    /**
     * Emit GPS updates every 2 seconds for all drivers.
     * Uses random-walk algorithm to simulate realistic movement patterns.
     */
    @Scheduled(fixedRate = 2000, initialDelay = 2000)
    public void emitGpsUpdates() {
        if (!simulatorEnabled) {
            return;
        }

        driverStates.forEach((driverId, state) -> {
            // Random walk: small random movements in lat/lng
            double latDelta = (random.nextDouble() - 0.5) * MAX_STEP;
            double lngDelta = (random.nextDouble() - 0.5) * MAX_STEP;

            state.lat += latDelta;
            state.lng += lngDelta;

            // Keep drivers within bounds (bounce back if they go too far)
            if (state.lat < BASE_LAT - LAT_RANGE / 2) {
                state.lat = BASE_LAT - LAT_RANGE / 2;
            } else if (state.lat > BASE_LAT + LAT_RANGE / 2) {
                state.lat = BASE_LAT + LAT_RANGE / 2;
            }

            if (state.lng < BASE_LNG - LNG_RANGE / 2) {
                state.lng = BASE_LNG - LNG_RANGE / 2;
            } else if (state.lng > BASE_LNG + LNG_RANGE / 2) {
                state.lng = BASE_LNG + LNG_RANGE / 2;
            }

            // Randomly adjust speed (gradual changes)
            double speedDelta = (random.nextDouble() - 0.5) * 10;
            state.speed = Math.max(MIN_SPEED, Math.min(MAX_SPEED, state.speed + speedDelta));

            // Create and send event
            DriverLocationEvent event = new DriverLocationEvent(
                driverId,
                state.lat,
                state.lng,
                System.currentTimeMillis(),
                state.speed
            );

            // Key by driverId to ensure ordering per driver
            kafkaTemplate.send(TOPIC, driverId, event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to send location for {}: {}", driverId, ex.getMessage());
                    } else {
                        log.debug("Sent location for {} to partition {}",
                            driverId,
                            result.getRecordMetadata().partition());
                    }
                });
        });

        log.info("Emitted GPS updates for {} drivers", driverStates.size());
    }

    /**
     * Internal state for each simulated driver
     */
    private static class DriverState {
        double lat;
        double lng;
        double speed;

        DriverState(double lat, double lng, double speed) {
            this.lat = lat;
            this.lng = lng;
            this.speed = speed;
        }
    }
}
