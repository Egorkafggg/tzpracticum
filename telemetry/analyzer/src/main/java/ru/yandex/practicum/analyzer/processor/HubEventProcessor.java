package ru.yandex.practicum.analyzer.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.analyzer.service.HubEventService;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

@Slf4j
@Component
@RequiredArgsConstructor
public class HubEventProcessor implements Runnable {

    private static final String TOPIC = "telemetry.hubs.v1";

    @Qualifier("hubEventConsumerProperties")
    private final Properties consumerProperties;

    private final HubEventService hubEventService;

    private Consumer<String, HubEventAvro> consumer;

    @Override
    public void run() {
        consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(List.of(TOPIC));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown hook triggered, waking up consumer");
            consumer.wakeup();
        }));

        try {
            while (true) {
                ConsumerRecords<String, HubEventAvro> records =
                        consumer.poll(Duration.ofMillis(1000));
                records.forEach(record -> {
                    log.info("Received hub event: {}", record.value());
                    hubEventService.process(record.value());
                });
            }
        } catch (WakeupException e) {
            log.info("Consumer wakeup, shutting down");
        } finally {
            consumer.close();
            log.info("Hub event consumer closed");
        }
    }
}
