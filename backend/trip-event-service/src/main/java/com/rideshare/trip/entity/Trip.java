package com.rideshare.trip.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "trips")
public class Trip {

    @Id
    @Column(name = "trip_id")
    private String tripId;

    @Column(name = "driver_id", nullable = false)
    private String driverId;

    @Column(name = "rider_id", nullable = false)
    private String riderId;

    @Column(name = "origin_lat", nullable = false)
    private double originLat;

    @Column(name = "origin_lng", nullable = false)
    private double originLng;

    @Column(name = "dest_lat", nullable = false)
    private double destLat;

    @Column(name = "dest_lng", nullable = false)
    private double destLng;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private TripStatus status;

    @Column(name = "started_at")
    private Instant startedAt;

    @Column(name = "ended_at")
    private Instant endedAt;

    protected Trip() {
        // required by JPA
    }

    public Trip(String tripId, String driverId, String riderId,
                double originLat, double originLng, double destLat, double destLng) {
        this.tripId = tripId;
        this.driverId = driverId;
        this.riderId = riderId;
        this.originLat = originLat;
        this.originLng = originLng;
        this.destLat = destLat;
        this.destLng = destLng;
        this.status = TripStatus.STARTED;
        this.startedAt = Instant.now();
    }

    public void markEnded() {
        this.status = TripStatus.ENDED;
        this.endedAt = Instant.now();
    }

    public String getTripId() {
        return tripId;
    }

    public String getDriverId() {
        return driverId;
    }

    public String getRiderId() {
        return riderId;
    }

    public double getOriginLat() {
        return originLat;
    }

    public double getOriginLng() {
        return originLng;
    }

    public double getDestLat() {
        return destLat;
    }

    public double getDestLng() {
        return destLng;
    }

    public TripStatus getStatus() {
        return status;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public Instant getEndedAt() {
        return endedAt;
    }
}
