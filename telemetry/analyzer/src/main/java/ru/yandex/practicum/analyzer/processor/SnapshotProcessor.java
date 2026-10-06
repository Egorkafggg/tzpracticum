package ru.yandex.practicum.analyzer.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.analyzer.service.SnapshotService;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotProcessor {

    private static final String TOPIC = "telemetry.snapshots.v1";

    @Qualifier("snapshotConsumerProperties")
    private final Properties consumerProperties;

    private final SnapshotService snapshotService;

    private Consumer<String, SensorsSnapshotAvro> consumer;

    public void start() {
        consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(List.of(TOPIC));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown hook triggered, waking up snapshot consumer");
            consumer.wakeup();
        }));

        try {
            while (true) {
                ConsumerRecords<String, SensorsSnapshotAvro> records =
                        consumer.poll(Duration.ofMillis(1000));
                records.forEach(record -> {
                    log.info("Received snapshot for hub {}", record.value().getHubId());
                    snapshotService.process(record.value());
                });
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        } catch (WakeupException e) {
            log.info("Snapshot consumer wakeup, shutting down");
        } finally {
            consumer.close();
            log.info("Snapshot consumer closed");
        }
    }
}
