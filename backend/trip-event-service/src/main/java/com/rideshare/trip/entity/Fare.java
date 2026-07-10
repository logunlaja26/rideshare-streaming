package com.rideshare.trip.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "fares")
public class Fare {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "trip_id", nullable = false)
    private String tripId;

    @Column(name = "amount", nullable = false)
    private double amount;

    @Column(name = "calculated_at", nullable = false)
    private Instant calculatedAt;

    protected Fare() {
        // required by JPA
    }

    public Fare(String tripId, double amount) {
        this.tripId = tripId;
        this.amount = amount;
        this.calculatedAt = Instant.now();
    }

    public Long getId() {
        return id;
    }

    public String getTripId() {
        return tripId;
    }

    public double getAmount() {
        return amount;
    }

    public Instant getCalculatedAt() {
        return calculatedAt;
    }
}
