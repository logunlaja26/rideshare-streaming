package com.rideshare.location.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Redis configuration for storing driver locations.
 * Uses HASH data structure: key = "driver:locations", field = driverId, value = DriverLocationEvent JSON
 *
 * Note: We use StringRedisTemplate (Spring Boot autoconfigured) with manual JSON serialization
 * for simplicity and reliability.
 */
@Configuration
public class RedisConfig {

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}
