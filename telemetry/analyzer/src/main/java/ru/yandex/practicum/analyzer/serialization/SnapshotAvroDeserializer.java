package ru.yandex.practicum.analyzer.serialization;

import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

public class SnapshotAvroDeserializer extends BaseAvroDeserializer<SensorsSnapshotAvro> {
    public SnapshotAvroDeserializer() {
        super(SensorsSnapshotAvro.getClassSchema());
    }
}
