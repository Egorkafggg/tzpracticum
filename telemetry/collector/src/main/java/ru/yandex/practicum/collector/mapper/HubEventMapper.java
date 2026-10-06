package ru.yandex.practicum.collector.mapper;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.dto.hub.HubEvent;
import ru.yandex.practicum.collector.dto.hub.device.DeviceAddedEvent;
import ru.yandex.practicum.collector.dto.hub.device.DeviceRemovedEvent;
import ru.yandex.practicum.collector.dto.hub.device.DeviceType;
import ru.yandex.practicum.collector.dto.hub.scenario.DeviceAction;
import ru.yandex.practicum.collector.dto.hub.scenario.ScenarioAddedEvent;
import ru.yandex.practicum.collector.dto.hub.scenario.ScenarioCondition;
import ru.yandex.practicum.collector.dto.hub.scenario.ScenarioRemovedEvent;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Instant;
import java.util.List;

@Component
public class HubEventMapper {

    public HubEventAvro map(HubEvent event) {
        return HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(safeTimestamp(event.getTimestamp()))
                .setPayload(mapPayload(event))
                .build();
    }

    private Object mapPayload(HubEvent event) {
        return switch (event.getType()) {
            case DEVICE_ADDED -> mapDeviceAdded((DeviceAddedEvent) event);
            case DEVICE_REMOVED -> mapDeviceRemoved((DeviceRemovedEvent) event);
            case SCENARIO_ADDED -> mapScenarioAdded((ScenarioAddedEvent) event);
            case SCENARIO_REMOVED -> mapScenarioRemoved((ScenarioRemovedEvent) event);
        };
    }

    private ScenarioRemovedEventAvro mapScenarioRemoved(ScenarioRemovedEvent scenarioRemovedEvent) {
        return ScenarioRemovedEventAvro.newBuilder()
                .setName(scenarioRemovedEvent.getName())
                .build();
    }

    private ScenarioAddedEventAvro mapScenarioAdded(ScenarioAddedEvent scenarioAddedEvente) {
        return ScenarioAddedEventAvro.newBuilder()
                .setName(scenarioAddedEvente.getName())
                .setConditions(mapConditions(scenarioAddedEvente.getConditions()))
                .setActions(mapActions(scenarioAddedEvente.getActions()))
                .build();
    }

    private List<ScenarioConditionAvro> mapConditions(List<ScenarioCondition> conditions) {
        return conditions.stream()
                .map(this::mapCondition)
                .toList();
    }

    private ScenarioConditionAvro mapCondition(ScenarioCondition scenarioCondition) {
        return ScenarioConditionAvro.newBuilder()
                .setSensorId(scenarioCondition.getSensorId())
                .setType(scenarioCondition.getType() != null ? ConditionTypeAvro.valueOf(scenarioCondition.getType().name()) : null)
                .setOperation(scenarioCondition.getOperation() != null ? ConditionOperationAvro.valueOf(scenarioCondition.getOperation().name()) : null)
                .setValue(scenarioCondition.getValue())
                .build();
    }

    private List<DeviceActionAvro> mapActions(List<DeviceAction> actions) {
        return actions.stream()
                .map(this::mapAction)
                .toList();
    }

    private DeviceActionAvro mapAction(DeviceAction deviceAction) {
        return DeviceActionAvro.newBuilder()
                .setSensorId(deviceAction.getSensorId())
                .setType(deviceAction.getType() != null ? ActionTypeAvro.valueOf(deviceAction.getType().name()) : null)
                .setValue(deviceAction.getValue()) // Integer, допускает null
                .build();
    }

    private DeviceAddedEventAvro mapDeviceAdded(DeviceAddedEvent deviceAddedEvent) {
        return DeviceAddedEventAvro.newBuilder()
                .setId(deviceAddedEvent.getId())
                .setType(deviceAddedEvent.getDeviceType() != null ? mapDeviceType(deviceAddedEvent.getDeviceType()) : null)
                .build();
    }

    private DeviceRemovedEventAvro mapDeviceRemoved(DeviceRemovedEvent deviceRemovedEvent) {
        return DeviceRemovedEventAvro.newBuilder()
                .setId(deviceRemovedEvent.getId())
                .build();
    }

    private DeviceTypeAvro mapDeviceType(DeviceType type) {
        return DeviceTypeAvro.valueOf(type.name());
    }

    /** Avro-поле timestamp объявлено как timestamp-millis → Instant. */
    private Instant safeTimestamp(Instant instant) {
        return instant != null ? instant : Instant.now();
    }
}