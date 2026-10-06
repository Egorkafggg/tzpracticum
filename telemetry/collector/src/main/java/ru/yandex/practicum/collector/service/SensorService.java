package ru.yandex.practicum.collector.service;

import ru.yandex.practicum.collector.dto.sensor.SensorEvent;
import ru.yandex.practicum.collector.kafka.producer.KafkaEventProducer;
import lombok.RequiredArgsConstructor;
import ru.yandex.practicum.collector.mapper.SensorEventMapper;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

@Service
@RequiredArgsConstructor
public class SensorService {
    private final static String TOPIC = "telemetry.sensors.v1";

    private final SensorEventMapper mapper;
    private final KafkaEventProducer producer;

    public void handle(SensorEvent sensorEvent) {
        SensorEventAvro sensorEventAvro = mapper.mapping(sensorEvent);

        producer.send(TOPIC, sensorEvent.getHubId(), sensorEventAvro);
    }
}
