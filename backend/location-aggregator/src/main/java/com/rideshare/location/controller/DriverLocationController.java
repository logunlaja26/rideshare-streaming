package com.rideshare.location.controller;

import com.rideshare.location.model.GeoJsonFeatureCollection;
import com.rideshare.location.service.LocationAggregatorService;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST API for querying driver locations.
 * Serves real-time driver positions from Redis cache as GeoJSON.
 */
@RestController
@RequestMapping("/api/drivers")
public class DriverLocationController {

    private final LocationAggregatorService locationService;

    public DriverLocationController(LocationAggregatorService locationService) {
        this.locationService = locationService;
    }

    /**
     * Returns all active driver locations as a GeoJSON FeatureCollection.
     * Used by the frontend map to display driver positions in real-time.
     *
     * Example response:
     * {
     *   "type": "FeatureCollection",
     *   "features": [
     *     {
     *       "type": "Feature",
     *       "geometry": {
     *         "type": "Point",
     *         "coordinates": [-122.4194, 37.7749]
     *       },
     *       "properties": {
     *         "driverId": "driver-001",
     *         "speed": 45.5,
     *         "timestamp": 1234567890
     *       }
     *     }
     *   ]
     * }
     *
     * @return GeoJSON FeatureCollection containing all driver locations
     */
    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public GeoJsonFeatureCollection getAllDrivers() {
        return locationService.getAllDriverLocations();
    }
}
