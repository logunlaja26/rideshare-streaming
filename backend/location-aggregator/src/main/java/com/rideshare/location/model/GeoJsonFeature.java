package com.rideshare.location.model;

/**
 * GeoJSON Feature representing a single driver location.
 * Follows the GeoJSON RFC 7946 specification.
 *
 * Example:
 * {
 *   "type": "Feature",
 *   "geometry": {
 *     "type": "Point",
 *     "coordinates": [-122.4194, 37.7749]
 *   },
 *   "properties": {
 *     "driverId": "driver-001",
 *     "speed": 45.5,
 *     "timestamp": 1234567890
 *   }
 * }
 */
public record GeoJsonFeature(
    String type,
    GeoJsonPoint geometry,
    DriverProperties properties
) {
    public GeoJsonFeature(GeoJsonPoint geometry, DriverProperties properties) {
        this("Feature", geometry, properties);
    }
}
