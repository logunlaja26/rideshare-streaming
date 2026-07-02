package com.rideshare.gps.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DriverLocationEventTest {

    @Test
    void shouldCreateValidDriverLocationEvent() {
        // When: Creating a valid event
        DriverLocationEvent event = new DriverLocationEvent(
            "driver-001",
            37.7749,
            -122.4194,
            System.currentTimeMillis(),
            45.5
        );

        // Then: Event should be created successfully
        assertThat(event.driverId()).isEqualTo("driver-001");
        assertThat(event.lat()).isEqualTo(37.7749);
        assertThat(event.lng()).isEqualTo(-122.4194);
        assertThat(event.speed()).isEqualTo(45.5);
        assertThat(event.timestamp()).isGreaterThan(0L);
    }

    @Test
    void shouldCreateEventWithFactoryMethod() {
        // When: Creating event with factory method
        DriverLocationEvent event = DriverLocationEvent.create(
            "driver-002",
            37.7749,
            -122.4194,
            60.0
        );

        // Then: Event should have current timestamp
        assertThat(event.driverId()).isEqualTo("driver-002");
        assertThat(event.timestamp()).isCloseTo(
            System.currentTimeMillis(),
            org.assertj.core.data.Offset.offset(1000L)
        );
    }

    @Test
    void shouldRejectNullDriverId() {
        assertThatThrownBy(() -> new DriverLocationEvent(
            null,
            37.7749,
            -122.4194,
            System.currentTimeMillis(),
            45.5
        ))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("driverId cannot be null");
    }

    @Test
    void shouldRejectBlankDriverId() {
        assertThatThrownBy(() -> new DriverLocationEvent(
            "   ",
            37.7749,
            -122.4194,
            System.currentTimeMillis(),
            45.5
        ))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("driverId cannot be null or blank");
    }

    @Test
    void shouldRejectInvalidLatitude() {
        // Latitude too low
        assertThatThrownBy(() -> new DriverLocationEvent(
            "driver-001",
            -91.0,
            -122.4194,
            System.currentTimeMillis(),
            45.5
        ))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Latitude must be between -90 and 90");

        // Latitude too high
        assertThatThrownBy(() -> new DriverLocationEvent(
            "driver-001",
            91.0,
            -122.4194,
            System.currentTimeMillis(),
            45.5
        ))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Latitude must be between -90 and 90");
    }

    @Test
    void shouldRejectInvalidLongitude() {
        // Longitude too low
        assertThatThrownBy(() -> new DriverLocationEvent(
            "driver-001",
            37.7749,
            -181.0,
            System.currentTimeMillis(),
            45.5
        ))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Longitude must be between -180 and 180");

        // Longitude too high
        assertThatThrownBy(() -> new DriverLocationEvent(
            "driver-001",
            37.7749,
            181.0,
            System.currentTimeMillis(),
            45.5
        ))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Longitude must be between -180 and 180");
    }

    @Test
    void shouldRejectNegativeSpeed() {
        assertThatThrownBy(() -> new DriverLocationEvent(
            "driver-001",
            37.7749,
            -122.4194,
            System.currentTimeMillis(),
            -10.0
        ))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Speed cannot be negative");
    }

    @Test
    void shouldAcceptZeroSpeed() {
        // When: Creating event with zero speed (stopped driver)
        DriverLocationEvent event = new DriverLocationEvent(
            "driver-001",
            37.7749,
            -122.4194,
            System.currentTimeMillis(),
            0.0
        );

        // Then: Event should be created successfully
        assertThat(event.speed()).isEqualTo(0.0);
    }

    @Test
    void shouldAcceptBoundaryValues() {
        // When: Creating event with boundary values
        DriverLocationEvent event = new DriverLocationEvent(
            "driver-001",
            90.0,    // Max latitude
            180.0,   // Max longitude
            System.currentTimeMillis(),
            0.0      // Min speed
        );

        // Then: Event should be created successfully
        assertThat(event.lat()).isEqualTo(90.0);
        assertThat(event.lng()).isEqualTo(180.0);
        assertThat(event.speed()).isEqualTo(0.0);
    }
}
