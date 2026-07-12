package com.rideshare.tripproducer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class TripProducerApplication {

    public static void main(String[] args) {
        SpringApplication.run(TripProducerApplication.class, args);
    }
}
