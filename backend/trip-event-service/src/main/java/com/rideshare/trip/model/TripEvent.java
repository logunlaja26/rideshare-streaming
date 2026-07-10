package com.rideshare.trip.model;

import java.time.Instant;

/**
 * Represents a trip lifecycle event produced to and consumed from the trip.events Kafka topic.
 * The same record shape carries TRIP_STARTED, TRIP_ENDED and FARE_CALCULATED — consumers switch
 * on eventType. fareAmount is only populated for FARE_CALCULATED.
 *
 * @param tripId Unique identifier for the trip (used as Kafka message key)
 * @param eventType Which stage of the trip lifecycle this event represents
 * @param driverId Driver assigned to the trip
 * @param riderId Rider who requested the trip
 * @param originLat Pickup latitude
 * @param originLng Pickup longitude
 * @param destLat Drop-off latitude
 * @param destLng Drop-off longitude
 * @param fareAmount Fare in USD, only set when eventType is FARE_CALCULATED
 * @param timestamp Unix timestamp (milliseconds) when the event occurred
 */
public record TripEvent(
    String tripId,
    TripEventType eventType,
    String driverId,
    String riderId,
    double originLat,
    double originLng,
    double destLat,
    double destLng,
    Double fareAmount,
    long timestamp
) {
    public TripEvent {
        if (tripId == null || tripId.isBlank()) {
            throw new IllegalArgumentException("tripId cannot be null or blank");
        }
        if (eventType == null) {
            throw new IllegalArgumentException("eventType cannot be null");
        }
        if (driverId == null || driverId.isBlank()) {
            throw new IllegalArgumentException("driverId cannot be null or blank");
        }
        if (riderId == null || riderId.isBlank()) {
            throw new IllegalArgumentException("riderId cannot be null or blank");
        }
    }

    public static TripEvent started(String tripId, String driverId, String riderId,
                                     double originLat, double originLng,
                                     double destLat, double destLng) {
        return new TripEvent(tripId, TripEventType.TRIP_STARTED, driverId, riderId,
            originLat, originLng, destLat, destLng, null, Instant.now().toEpochMilli());
    }

    public static TripEvent ended(String tripId, String driverId, String riderId,
                                   double originLat, double originLng,
                                   double destLat, double destLng) {
        return new TripEvent(tripId, TripEventType.TRIP_ENDED, driverId, riderId,
            originLat, originLng, destLat, destLng, null, Instant.now().toEpochMilli());
    }

    public static TripEvent fareCalculated(String tripId, String driverId, String riderId,
                                            double originLat, double originLng,
                                            double destLat, double destLng, double fareAmount) {
        return new TripEvent(tripId, TripEventType.FARE_CALCULATED, driverId, riderId,
            originLat, originLng, destLat, destLng, fareAmount, Instant.now().toEpochMilli());
    }
}
