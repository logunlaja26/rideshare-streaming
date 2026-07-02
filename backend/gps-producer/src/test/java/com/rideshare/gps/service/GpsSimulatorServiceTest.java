package com.rideshare.gps.service;

import com.rideshare.gps.model.DriverLocationEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@SpringBootTest
@TestPropertySource(properties = {
    "gps.simulator.enabled=true",
    "spring.kafka.bootstrap-servers=localhost:9999"
})
class GpsSimulatorServiceTest {

    @MockBean
    private KafkaTemplate<String, DriverLocationEvent> kafkaTemplate;

    @Autowired
    private GpsSimulatorService gpsSimulatorService;

    @Test
    void shouldEmitGpsUpdatesForAllDrivers() {
        // Given: Kafka template is mocked to return successful futures
        CompletableFuture<SendResult<String, DriverLocationEvent>> future = new CompletableFuture<>();
        ProducerRecord<String, DriverLocationEvent> producerRecord =
            new ProducerRecord<>("driver.location", 0, "driver-001", null);
        SendResult<String, DriverLocationEvent> sendResult = new SendResult<>(producerRecord, null);
        future.complete(sendResult);

        when(kafkaTemplate.send(eq("driver.location"), any(String.class), any(DriverLocationEvent.class)))
            .thenReturn(future);

        // When: emitting GPS updates
        gpsSimulatorService.emitGpsUpdates();

        // Then: 50 events should be sent to Kafka
        verify(kafkaTemplate, times(50)).send(
            eq("driver.location"),
            any(String.class),
            any(DriverLocationEvent.class)
        );
    }

    @Test
    void shouldKeyMessagesByDriverId() {
        // Given: Kafka template is mocked
        CompletableFuture<SendResult<String, DriverLocationEvent>> future = new CompletableFuture<>();
        ProducerRecord<String, DriverLocationEvent> producerRecord =
            new ProducerRecord<>("driver.location", 0, "driver-001", null);
        SendResult<String, DriverLocationEvent> sendResult = new SendResult<>(producerRecord, null);
        future.complete(sendResult);

        when(kafkaTemplate.send(eq("driver.location"), any(String.class), any(DriverLocationEvent.class)))
            .thenReturn(future);

        // When: emitting GPS updates
        gpsSimulatorService.emitGpsUpdates();

        // Then: each message key should match the driverId in the event
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<DriverLocationEvent> eventCaptor = ArgumentCaptor.forClass(DriverLocationEvent.class);

        verify(kafkaTemplate, times(50)).send(
            eq("driver.location"),
            keyCaptor.capture(),
            eventCaptor.capture()
        );

        List<String> keys = keyCaptor.getAllValues();
        List<DriverLocationEvent> events = eventCaptor.getAllValues();

        // Verify each key matches the event's driverId
        for (int i = 0; i < keys.size(); i++) {
            assertThat(keys.get(i)).isEqualTo(events.get(i).driverId());
            assertThat(keys.get(i)).matches("driver-\\d{3}");
        }
    }

    @Test
    void shouldGenerateValidLocationData() {
        // Given: Kafka template is mocked
        CompletableFuture<SendResult<String, DriverLocationEvent>> future = new CompletableFuture<>();
        ProducerRecord<String, DriverLocationEvent> producerRecord =
            new ProducerRecord<>("driver.location", 0, "driver-001", null);
        SendResult<String, DriverLocationEvent> sendResult = new SendResult<>(producerRecord, null);
        future.complete(sendResult);

        when(kafkaTemplate.send(eq("driver.location"), any(String.class), any(DriverLocationEvent.class)))
            .thenReturn(future);

        // When: emitting GPS updates
        gpsSimulatorService.emitGpsUpdates();

        // Then: all events should have valid coordinates and speed
        ArgumentCaptor<DriverLocationEvent> eventCaptor = ArgumentCaptor.forClass(DriverLocationEvent.class);
        verify(kafkaTemplate, times(50)).send(
            eq("driver.location"),
            any(String.class),
            eventCaptor.capture()
        );

        List<DriverLocationEvent> events = eventCaptor.getAllValues();
        for (DriverLocationEvent event : events) {
            // San Francisco area bounds
            assertThat(event.lat()).isBetween(37.74, 37.81);
            assertThat(event.lng()).isBetween(-122.45, -122.39);
            assertThat(event.speed()).isBetween(0.0, 80.0);
            assertThat(event.timestamp()).isGreaterThan(0L);
        }
    }

    @Test
    void shouldUpdateDriverPositionsOnSubsequentCalls() {
        // Given: Kafka template is mocked
        CompletableFuture<SendResult<String, DriverLocationEvent>> future = new CompletableFuture<>();
        ProducerRecord<String, DriverLocationEvent> producerRecord =
            new ProducerRecord<>("driver.location", 0, "driver-001", null);
        SendResult<String, DriverLocationEvent> sendResult = new SendResult<>(producerRecord, null);
        future.complete(sendResult);

        when(kafkaTemplate.send(eq("driver.location"), any(String.class), any(DriverLocationEvent.class)))
            .thenReturn(future);

        // When: emitting updates twice
        gpsSimulatorService.emitGpsUpdates();

        ArgumentCaptor<DriverLocationEvent> firstBatch = ArgumentCaptor.forClass(DriverLocationEvent.class);
        verify(kafkaTemplate, times(50)).send(
            eq("driver.location"),
            any(String.class),
            firstBatch.capture()
        );

        gpsSimulatorService.emitGpsUpdates();

        ArgumentCaptor<DriverLocationEvent> secondBatch = ArgumentCaptor.forClass(DriverLocationEvent.class);
        verify(kafkaTemplate, times(100)).send(
            eq("driver.location"),
            any(String.class),
            secondBatch.capture()
        );

        // Then: positions should be different (drivers moved)
        List<DriverLocationEvent> first = firstBatch.getAllValues();
        List<DriverLocationEvent> second = secondBatch.getAllValues();

        // At least some drivers should have different coordinates
        boolean somePositionsChanged = false;
        for (int i = 0; i < 50; i++) {
            DriverLocationEvent event1 = first.get(i);
            DriverLocationEvent event2 = second.get(50 + i);

            if (event1.lat() != event2.lat() || event1.lng() != event2.lng()) {
                somePositionsChanged = true;
                break;
            }
        }

        assertThat(somePositionsChanged).isTrue();
    }
}
