package com.rideshare.location.model;

import java.util.List;

/**
 * GeoJSON Point geometry representing a geographic coordinate.
 * Coordinates are in [longitude, latitude] order per GeoJSON spec.
 *
 * Example:
 * {
 *   "type": "Point",
 *   "coordinates": [-122.4194, 37.7749]
 * }
 */
public record GeoJsonPoint(
    String type,
    List<Double> coordinates
) {
    public GeoJsonPoint(double lng, double lat) {
        this("Point", List.of(lng, lat));
    }
}
