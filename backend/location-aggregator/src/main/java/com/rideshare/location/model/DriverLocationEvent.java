package com.rideshare.location.model;

/**
 * Represents a GPS location update from a driver.
 * Consumed from the driver.location Kafka topic.
 *
 * @param driverId Unique identifier for the driver (used as Kafka message key)
 * @param lat Latitude coordinate
 * @param lng Longitude coordinate
 * @param timestamp Unix timestamp (milliseconds) when location was captured
 * @param speed Current speed in km/h
 */
public record DriverLocationEvent(
    String driverId,
    double lat,
    double lng,
    long timestamp,
    double speed
) {
    public DriverLocationEvent {
        if (driverId == null || driverId.isBlank()) {
            throw new IllegalArgumentException("driverId cannot be null or blank");
        }
        if (lat < -90 || lat > 90) {
            throw new IllegalArgumentException("Latitude must be between -90 and 90");
        }
        if (lng < -180 || lng > 180) {
            throw new IllegalArgumentException("Longitude must be between -180 and 180");
        }
        if (speed < 0) {
            throw new IllegalArgumentException("Speed cannot be negative");
        }
    }
}
