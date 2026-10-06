package ru.yandex.practicum.collector.service;

import ru.yandex.practicum.collector.dto.hub.HubEvent;
import ru.yandex.practicum.collector.kafka.producer.KafkaEventProducer;
import lombok.RequiredArgsConstructor;
import ru.yandex.practicum.collector.mapper.HubEventMapper;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

@Service
@RequiredArgsConstructor
public class HubService {
    private final static String TOPIC = "telemetry.hubs.v1";

    private final HubEventMapper mapper;
    private final KafkaEventProducer producer;

    public void handle(HubEvent hubEvent) {
        HubEventAvro hubEventAvro = mapper.map(hubEvent);

        producer.send(TOPIC, hubEvent.getHubId(), hubEventAvro);
    }
}
