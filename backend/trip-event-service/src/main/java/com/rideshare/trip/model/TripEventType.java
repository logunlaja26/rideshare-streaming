package com.rideshare.trip.model;

/**
 * Types of trip lifecycle events published to the trip.events Kafka topic.
 */
public enum TripEventType {
    TRIP_STARTED,
    TRIP_ENDED,
    FARE_CALCULATED
}
