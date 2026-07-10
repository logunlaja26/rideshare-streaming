package com.rideshare.trip.service;

import com.rideshare.trip.model.TripEvent;
import com.rideshare.trip.model.TripEventType;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TripSimulatorServiceTest {

    @Mock
    private KafkaTemplate<String, TripEvent> kafkaTemplate;

    private TripSimulatorService tripSimulatorService;

    @BeforeEach
    void setUp() {
        tripSimulatorService = new TripSimulatorService(kafkaTemplate);
        ReflectionTestUtils.setField(tripSimulatorService, "simulatorEnabled", true);

        ProducerRecord<String, TripEvent> producerRecord =
            new ProducerRecord<>("trip.events", 0, "trip-1", null);
        SendResult<String, TripEvent> sendResult = new SendResult<>(producerRecord, null);
        CompletableFuture<SendResult<String, TripEvent>> future = new CompletableFuture<>();
        future.complete(sendResult);

        when(kafkaTemplate.send(eq("trip.events"), any(String.class), any(TripEvent.class)))
            .thenReturn(future);
    }

    @Test
    void shouldEmitTripStartedEventsOnTick() {
        tripSimulatorService.tick();

        ArgumentCaptor<TripEvent> eventCaptor = ArgumentCaptor.forClass(TripEvent.class);
        verify(kafkaTemplate, atLeastOnce()).send(eq("trip.events"), any(String.class), eventCaptor.capture());

        List<TripEvent> events = eventCaptor.getAllValues();
        assertThat(events).isNotEmpty();
        assertThat(events).allMatch(e -> e.eventType() == TripEventType.TRIP_STARTED);
    }

    @Test
    void shouldKeyMessagesByTripId() {
        tripSimulatorService.tick();

        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<TripEvent> eventCaptor = ArgumentCaptor.forClass(TripEvent.class);
        verify(kafkaTemplate, atLeastOnce()).send(eq("trip.events"), keyCaptor.capture(), eventCaptor.capture());

        List<String> keys = keyCaptor.getAllValues();
        List<TripEvent> events = eventCaptor.getAllValues();

        for (int i = 0; i < keys.size(); i++) {
            assertThat(keys.get(i)).isEqualTo(events.get(i).tripId());
        }
    }

    @Test
    void shouldGenerateValidTripData() {
        tripSimulatorService.tick();

        ArgumentCaptor<TripEvent> eventCaptor = ArgumentCaptor.forClass(TripEvent.class);
        verify(kafkaTemplate, atLeastOnce()).send(eq("trip.events"), any(String.class), eventCaptor.capture());

        for (TripEvent event : eventCaptor.getAllValues()) {
            assertThat(event.tripId()).matches("trip-\\d+");
            assertThat(event.driverId()).matches("driver-\\d{3}");
            assertThat(event.riderId()).matches("rider-\\d{3}");
            assertThat(event.originLat()).isBetween(37.74, 37.81);
            assertThat(event.originLng()).isBetween(-122.45, -122.39);
            assertThat(event.timestamp()).isGreaterThan(0L);
        }
    }

    @Test
    void shouldEmitEndedAndFareCalculatedWhenTripDurationElapses() throws InterruptedException {
        // Force trips to be immediately due so the next tick completes them
        ReflectionTestUtils.setField(tripSimulatorService, "minTripSeconds", 0);
        ReflectionTestUtils.setField(tripSimulatorService, "maxTripSeconds", 0);

        tripSimulatorService.tick();
        Thread.sleep(20);
        tripSimulatorService.tick();

        ArgumentCaptor<TripEvent> eventCaptor = ArgumentCaptor.forClass(TripEvent.class);
        verify(kafkaTemplate, atLeastOnce()).send(eq("trip.events"), any(String.class), eventCaptor.capture());

        List<TripEvent> events = eventCaptor.getAllValues();
        assertThat(events).anyMatch(e -> e.eventType() == TripEventType.TRIP_ENDED);

        List<TripEvent> fareEvents = events.stream()
            .filter(e -> e.eventType() == TripEventType.FARE_CALCULATED)
            .toList();
        assertThat(fareEvents).isNotEmpty();
        assertThat(fareEvents).allMatch(e -> e.fareAmount() != null && e.fareAmount() > 0);
    }
}
