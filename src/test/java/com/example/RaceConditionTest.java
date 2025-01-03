package com.example;

import io.micronaut.configuration.kafka.ConsumerRegistry;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.micronaut.test.support.TestPropertyProvider;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.awaitility.Awaitility.await;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@MicronautTest
@Slf4j
class RaceConditionTest implements TestPropertyProvider {

    @Inject
    private ConsumerRegistry consumerRegistry;

    @Test
    void runUntilRaceCondition() {

        sleepFor100ms(); //adding this so resume isn't called too early

        consumerRegistry.getConsumerIds().forEach(consumerRegistry::resume);

        consumerRegistry.getConsumerIds().forEach(id -> {
            await().until(() -> consumerAssignment(id) != null);
            await().until(() -> !consumerRegistry.isPaused(id, consumerAssignment(id)));
        });
    }

    private static void sleepFor100ms() {
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private Set<TopicPartition> consumerAssignment(String id) {
        try {
            Set<TopicPartition> consumerAssignment = consumerRegistry.getConsumerAssignment(id);
            return consumerAssignment.isEmpty() ? null : consumerAssignment;
        } catch (Exception e) {
            return null;
        }
    }


    @Container
    static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.3.3")
    );

    @Override
    public @NonNull Map<String, String> getProperties() {
        if (!kafka.isRunning()) {
            kafka.start();
        }
        initKafka();
        return Map.of("kafka.bootstrap.servers", kafka.getBootstrapServers());
    }

    private static void initKafka() {
        if (!kafka.isRunning()) {
            kafka.start();
        }
        try (var adminClient = AdminClient.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            adminClient.createTopics(List.of(new NewTopic("topic-name", 1, (short) 1)));
        }
    }

}
