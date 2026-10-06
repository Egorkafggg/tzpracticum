package ru.yandex.practicum.analyzer.model;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.FieldDefaults;

@Entity
@Table(name = "conditions")
@Getter
@Setter
@FieldDefaults(level = AccessLevel.PRIVATE)
public class Condition {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    Long id;

    @ManyToOne
    @JoinColumn(name = "scenario_id")
    Scenario scenario;

    @Enumerated(EnumType.STRING)
    ConditionType type;

    @Enumerated(EnumType.STRING)
    ConditionOperation operation;

    Integer value;
}
