package ru.yandex.practicum.analyzer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.analyzer.model.*;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;
import ru.yandex.practicum.kafka.telemetry.event.*;

import com.google.protobuf.Timestamp;

import java.time.Instant;
import java.util.List;

import static ru.yandex.practicum.kafka.telemetry.event.ConditionOperationAvro.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class ScenarioExecutor {

    @GrpcClient("hub-router")
    private HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterClient;

    public void execute(SensorsSnapshotAvro snapshot, List<Scenario> scenarios) {
        scenarios.forEach(scenario -> {
            boolean allMatch = scenario.getConditions().stream()
                    .allMatch(condition -> checkCondition(snapshot, condition));

            if (allMatch) {
                log.info("Scenario '{}' matched for hub {}", scenario.getName(), snapshot.getHubId());
                scenario.getActions().forEach(action -> sendAction(snapshot, scenario, action));
            }
        });
    }

    private boolean checkCondition(SensorsSnapshotAvro snapshot, Condition condition) {
        return snapshot.getSensorsState().values().stream()
                .anyMatch(state -> matches(state, condition));
    }

    private boolean matches(SensorStateAvro state, Condition condition) {
        Object data = state.getData();
        Integer actual = extractValue(data, condition.getType());
        if (actual == null || condition.getValue() == null) {
            return false;
        }
        return switch (condition.getOperation()) {
            case EQUALS -> actual.equals(condition.getValue());
            case GREATER_THAN -> actual > condition.getValue();
            case LOWER_THAN -> actual < condition.getValue();
        };
    }

    private Integer extractValue(Object data, ConditionType type) {
        return switch (type) {
            case TEMPERATURE -> {
                if (data instanceof TemperatureSensorAvro t) yield t.getTemperatureC();
                if (data instanceof ClimateSensorAvro c) yield c.getTemperatureC();
                yield null;
            }
            case HUMIDITY -> data instanceof ClimateSensorAvro c ? c.getHumidity() : null;
            case CO2LEVEL -> data instanceof ClimateSensorAvro c ? c.getCo2Level() : null;
            case LUMINOSITY -> data instanceof LightSensorAvro l ? l.getLuminosity() : null;
            case MOTION -> data instanceof MotionSensorAvro m ? (m.getMotion() ? 1 : 0) : null;
            case SWITCH -> data instanceof SwitchSensorAvro s ? (s.getState() ? 1 : 0) : null;
        };
    }

    private void sendAction(SensorsSnapshotAvro snapshot, Scenario scenario, Action action) {
        DeviceActionProto actionProto = DeviceActionProto.newBuilder()
                .setSensorId(action.getSensorId())
                .setType(ActionTypeProto.valueOf(action.getType().name()))
                .setValue(action.getValue() != null ? action.getValue() : 0)
                .build();

        DeviceActionRequest request = DeviceActionRequest.newBuilder()
                .setHubId(snapshot.getHubId())
                .setScenarioName(scenario.getName())
                .setAction(actionProto)
                .setTimestamp(toProtoTimestamp(Instant.now()))
                .build();

        hubRouterClient.handleDeviceAction(request);
        log.info("Action sent: {}", request);
    }

    private Timestamp toProtoTimestamp(Instant instant) {
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }
}
