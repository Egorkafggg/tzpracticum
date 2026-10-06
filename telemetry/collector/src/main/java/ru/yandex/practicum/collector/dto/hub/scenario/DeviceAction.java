package ru.yandex.practicum.collector.dto.hub.scenario;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DeviceAction {

    private String sensorId;

    private ActionType type;

    private Integer value;
}
