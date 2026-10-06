package ru.yandex.practicum.analyzer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.analyzer.model.*;
import ru.yandex.practicum.analyzer.repo.ScenarioRepository;
import ru.yandex.practicum.analyzer.repo.SensorRepository;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class HubEventService {

    private final SensorRepository sensorRepository;
    private final ScenarioRepository scenarioRepository;

    @Transactional
    public void process(HubEventAvro event) {
        Object payload = event.getPayload();
        String hubId = event.getHubId();

        if (payload instanceof DeviceAddedEventAvro deviceAdded) {
            handleDeviceAdded(hubId, deviceAdded);
        } else if (payload instanceof DeviceRemovedEventAvro deviceRemoved) {
            handleDeviceRemoved(hubId, deviceRemoved);
        } else if (payload instanceof ScenarioAddedEventAvro scenarioAdded) {
            handleScenarioAdded(hubId, scenarioAdded);
        } else if (payload instanceof ScenarioRemovedEventAvro scenarioRemoved) {
            handleScenarioRemoved(hubId, scenarioRemoved);
        } else {
            log.warn("Unknown payload type: {}", payload.getClass());
        }
    }

    private void handleDeviceAdded(String hubId, DeviceAddedEventAvro event) {
        if (sensorRepository.findByIdAndHubId(event.getId(), hubId).isEmpty()) {
            Sensor sensor = new Sensor();
            sensor.setId(event.getId());
            sensor.setHubId(hubId);
            sensorRepository.save(sensor);
            log.info("Sensor added: {} for hub {}", event.getId(), hubId);
        } else {
            log.info("Sensor {} already exists in hub {}", event.getId(), hubId);
        }
    }

    private void handleDeviceRemoved(String hubId, DeviceRemovedEventAvro event) {
        sensorRepository.findByIdAndHubId(event.getId(), hubId)
                .ifPresent(sensorRepository::delete);
        log.info("Sensor removed: {} from hub {}", event.getId(), hubId);
    }

    private void handleScenarioAdded(String hubId, ScenarioAddedEventAvro event) {
        scenarioRepository.findByHubIdAndName(hubId, event.getName())
                .ifPresent(scenarioRepository::delete);

        Scenario scenario = new Scenario();
        scenario.setHubId(hubId);
        scenario.setName(event.getName());

        List<Condition> conditions = event.getConditions().stream()
                .map(c -> {
                    Condition condition = new Condition();
                    condition.setScenario(scenario);
                    condition.setType(ConditionType.valueOf(c.getType().name()));
                    condition.setOperation(ConditionOperation.valueOf(c.getOperation().name()));
                    condition.setValue(extractValue(c.getValue()));
                    return condition;
                })
                .toList();

        List<Action> actions = event.getActions().stream()
                .map(a -> {
                    Action action = new Action();
                    action.setScenario(scenario);
                    action.setSensorId(a.getSensorId());
                    action.setType(ActionType.valueOf(a.getType().name()));
                    action.setValue(a.getValue());
                    return action;
                })
                .toList();

        scenario.setConditions(conditions);
        scenario.setActions(actions);

        scenarioRepository.save(scenario);
        log.info("Scenario added: {} for hub {}", event.getName(), hubId);
    }

    private Integer extractValue(Object avroValue) {
        if (avroValue instanceof Integer i) {
            return i;
        }
        if (avroValue instanceof Boolean b) {
            return b ? 1 : 0;
        }
        return null;
    }

    private void handleScenarioRemoved(String hubId, ScenarioRemovedEventAvro event) {
        scenarioRepository.findByHubIdAndName(hubId, event.getName())
                .ifPresent(scenarioRepository::delete);
        log.info("Scenario removed: {} from hub {}", event.getName(), hubId);
    }
}
