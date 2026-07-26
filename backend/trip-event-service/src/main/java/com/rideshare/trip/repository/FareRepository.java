package com.rideshare.trip.repository;

import com.rideshare.trip.entity.Fare;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * Spring Data JPA repository for Fare entities.
 */
@Repository
public interface FareRepository extends JpaRepository<Fare, Long> {

    /**
     * Find a fare by its associated trip ID.
     *
     * @param tripId the trip ID
     * @return Optional containing the fare if found
     */
    Optional<Fare> findByTripId(String tripId);

    /**
     * Check if a fare exists for a given trip ID.
     *
     * @param tripId the trip ID
     * @return true if fare exists, false otherwise
     */
    boolean existsByTripId(String tripId);
}
