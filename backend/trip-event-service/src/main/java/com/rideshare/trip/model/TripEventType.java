package com.rideshare.trip.model;

/**
 * Lifecycle stages of a trip, in the order they are emitted to trip.events.
 */
public enum TripEventType {
    TRIP_STARTED,
    TRIP_ENDED,
    FARE_CALCULATED
}
