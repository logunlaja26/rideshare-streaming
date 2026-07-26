package com.rideshare.trip.repository;

import com.rideshare.trip.entity.Trip;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * Spring Data JPA repository for Trip entities.
 */
@Repository
public interface TripRepository extends JpaRepository<Trip, String> {

    /**
     * Find a trip by its tripId.
     *
     * @param tripId the trip ID
     * @return Optional containing the trip if found
     */
    Optional<Trip> findByTripId(String tripId);

    /**
     * Check if a trip exists by tripId.
     *
     * @param tripId the trip ID
     * @return true if trip exists, false otherwise
     */
    boolean existsByTripId(String tripId);
}
