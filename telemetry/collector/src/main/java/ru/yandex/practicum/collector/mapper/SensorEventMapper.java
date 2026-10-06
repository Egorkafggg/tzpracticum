package ru.yandex.practicum.collector.mapper;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.dto.sensor.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Instant;

@Component
public class SensorEventMapper {

    public SensorEventAvro mapping(SensorEvent sensorEvent) {
        if (sensorEvent == null) {
            return null;
        }

        return SensorEventAvro.newBuilder()
                .setId(sensorEvent.getId())
                .setHubId(sensorEvent.getHubId())
                .setTimestamp(safeTimestamp(sensorEvent.getTimestamp()))
                .setPayload(mapPayload(sensorEvent))
                .build();
    }

    private Object mapPayload(SensorEvent event) {
        return switch (event.getType()) {
            case LIGHT_SENSOR_EVENT -> mapLight((LightSensorEvent) event);
            case MOTION_SENSOR_EVENT -> mapMotion((MotionSensorEvent) event);
            case CLIMATE_SENSOR_EVENT -> mapClimate((ClimateSensorEvent) event);
            case SWITCH_SENSOR_EVENT -> mapSwitch((SwitchSensorEvent) event);
            case TEMPERATURE_SENSOR_EVENT -> mapTemperature((TemperatureSensorEvent) event);
        };
    }

    private LightSensorAvro mapLight(LightSensorEvent event) {
        return LightSensorAvro.newBuilder()
                .setLinkQuality(safeInt(event.getLinkQuality()))
                .setLuminosity(safeInt(event.getLuminosity()))
                .build();
    }

    private MotionSensorAvro mapMotion(MotionSensorEvent event) {
        return MotionSensorAvro.newBuilder()
                .setLinkQuality(safeInt(event.getLinkQuality()))
                .setMotion(safeBoolean(event.getMotion()))
                .setVoltage(safeInt(event.getVoltage()))
                .build();
    }

    private ClimateSensorAvro mapClimate(ClimateSensorEvent event) {
        return ClimateSensorAvro.newBuilder()
                .setCo2Level(safeInt(event.getCo2Level()))
                .setHumidity(safeInt(event.getHumidity()))
                .setTemperatureC(safeInt(event.getTemperatureC()))
                .build();
    }

    private SwitchSensorAvro mapSwitch(SwitchSensorEvent event) {
        return SwitchSensorAvro.newBuilder()
                .setState(safeBoolean(event.getState()))
                .build();
    }

    private TemperatureSensorAvro mapTemperature(TemperatureSensorEvent event) {
        return TemperatureSensorAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(safeTimestamp(event.getTimestamp()))
                .setTemperatureC(safeInt(event.getTemperatureC()))
                .setTemperatureF(safeInt(event.getTemperatureF()))
                .build();
    }

    private int safeInt(Integer value) {
        return value != null ? value : 0;
    }

    private boolean safeBoolean(Boolean value) {
        return value != null && value;
    }

    private Instant safeTimestamp(Instant instant) {
        return instant != null ? instant : Instant.now();
    }
}
