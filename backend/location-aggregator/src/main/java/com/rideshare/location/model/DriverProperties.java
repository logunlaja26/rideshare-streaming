package com.rideshare.location.model;

/**
 * Properties object for a driver location feature.
 * Contains metadata about the driver beyond their geographic position.
 */
public record DriverProperties(
    String driverId,
    double speed,
    long timestamp
) {
}
