package com.rideshare.location.model;

import java.util.List;

/**
 * GeoJSON FeatureCollection containing driver locations.
 * Follows the GeoJSON RFC 7946 specification.
 *
 * Example:
 * {
 *   "type": "FeatureCollection",
 *   "features": [...]
 * }
 */
public record GeoJsonFeatureCollection(
    String type,
    List<GeoJsonFeature> features
) {
    public GeoJsonFeatureCollection(List<GeoJsonFeature> features) {
        this("FeatureCollection", features);
    }
}
