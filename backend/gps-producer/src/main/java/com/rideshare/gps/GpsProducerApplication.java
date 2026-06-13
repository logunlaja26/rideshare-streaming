package com.rideshare.gps;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class GpsProducerApplication {

    public static void main(String[] args) {
        SpringApplication.run(GpsProducerApplication.class, args);
    }
}
