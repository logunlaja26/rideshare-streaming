package com.rideshare.dlq;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DlqProcessorApplication {

    public static void main(String[] args) {
        SpringApplication.run(DlqProcessorApplication.class, args);
    }
}
