package com.rideshare.tripproducer.model;

/**
 * Represents a trip lifecycle event produced to the trip.events Kafka topic.
 * Different event types carry different payloads.
 *
 * @param tripId Unique identifier for the trip (used as Kafka message key)
 * @param eventType Type of event (TRIP_STARTED, TRIP_ENDED, FARE_CALCULATED)
 * @param timestamp Unix timestamp (milliseconds) when event occurred
 * @param driverId Driver involved in this trip
 * @param riderId Rider involved in this trip
 * @param pickupLat Pickup location latitude (for TRIP_STARTED)
 * @param pickupLng Pickup location longitude (for TRIP_STARTED)
 * @param dropoffLat Dropoff location latitude (for TRIP_ENDED)
 * @param dropoffLng Dropoff location longitude (for TRIP_ENDED)
 * @param fareAmount Final fare in dollars (for FARE_CALCULATED)
 * @param distance Trip distance in kilometers (for TRIP_ENDED)
 * @param duration Trip duration in seconds (for TRIP_ENDED)
 */
public record TripEvent(
    String tripId,
    TripEventType eventType,
    long timestamp,
    String driverId,
    String riderId,
    Double pickupLat,
    Double pickupLng,
    Double dropoffLat,
    Double dropoffLng,
    Double fareAmount,
    Double distance,
    Integer duration
) {
    public TripEvent {
        if (tripId == null || tripId.isBlank()) {
            throw new IllegalArgumentException("tripId cannot be null or blank");
        }
        if (eventType == null) {
            throw new IllegalArgumentException("eventType cannot be null");
        }
    }

    // Factory methods for each event type

    public static TripEvent tripStarted(String tripId, String driverId, String riderId,
                                       double pickupLat, double pickupLng) {
        return new TripEvent(
            tripId,
            TripEventType.TRIP_STARTED,
            System.currentTimeMillis(),
            driverId,
            riderId,
            pickupLat,
            pickupLng,
            null, null, null, null, null
        );
    }

    public static TripEvent tripEnded(String tripId, double dropoffLat, double dropoffLng,
                                     double distance, int duration) {
        return new TripEvent(
            tripId,
            TripEventType.TRIP_ENDED,
            System.currentTimeMillis(),
            null, null, null, null,
            dropoffLat,
            dropoffLng,
            null,
            distance,
            duration
        );
    }

    public static TripEvent fareCalculated(String tripId, double fareAmount) {
        return new TripEvent(
            tripId,
            TripEventType.FARE_CALCULATED,
            System.currentTimeMillis(),
            null, null, null, null, null, null,
            fareAmount,
            null, null
        );
    }
}
