package com.example;

import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetStrategy;
import io.micronaut.configuration.kafka.annotation.Topic;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.UUID;

import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST;

@KafkaListener(
        clientId = "client-id",
        groupId = "group-id",
        offsetReset = EARLIEST,
        offsetStrategy = OffsetStrategy.DISABLED,
        autoStartup = false
)
public class RaceConditionListener {

    @Topic("topic-name")
    void listenForCommands(ConsumerRecord<UUID, Object> record) {

    }
}
