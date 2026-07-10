package com.rideshare.trip.repository;

import com.rideshare.trip.entity.Fare;
import org.springframework.data.jpa.repository.JpaRepository;

public interface FareRepository extends JpaRepository<Fare, Long> {
}
