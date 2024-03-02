package dev.hertlein.payground.kafkastreams;

import com.google.common.io.Resources;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public abstract class StreamingApp {

    private final Properties config;
    private final String outputTopic;
    private final String inputTopic;
    private final AtomicBoolean isShuttingDown = new AtomicBoolean(false);

    StreamingApp(Properties config, String inputTopic, String outputTopic) {
        this.config = config;
        this.outputTopic = outputTopic;
        this.inputTopic = inputTopic;

        Runtime.getRuntime().addShutdownHook(new Thread(() -> isShuttingDown.set(true)));
    }

    void run() {
        createTopics();
        startProducingThread(config, inputTopic, isShuttingDown);
        startConsumingThread();
        startStreaming(topology());
    }

    abstract void startProducingThread(Properties config, String topic, AtomicBoolean isShuttingDown);

    abstract Topology topology();

    @SneakyThrows
    void createTopics() {
        var topicNames = List.of(inputTopic, outputTopic);
        try (AdminClient client = AdminClient.create(config)) {
            var existingTopics = client.listTopics().names().get();

            var toBeCreatedTopics = topicNames.stream()
                    .filter(topicName -> !existingTopics.contains(topicName))
                    .map(topicName -> new NewTopic(topicName, 1, Short.parseShort("1")));

            toBeCreatedTopics.forEach(topic -> createTopic(topic, client));

            log.info("Topics {} available.", Arrays.asList(topicNames));
        }
    }

    @SneakyThrows
    private void createTopic(NewTopic topic, AdminClient client) {
        client.createTopics(List.of(topic)).all().get();
    }


    void startConsumingThread() {
        Thread.startVirtualThread(() ->
        {
            try (var consumer = new KafkaConsumer<Integer, String>(config)) {
                consumer.subscribe(List.of(outputTopic));
                log.info("Started Consuming from {}.", List.of(outputTopic));

                while (!isShuttingDown.get()) {
                    var records = consumer.poll(Duration.of(1, ChronoUnit.SECONDS));
                    if (!records.isEmpty()) {
                        log.info("=".repeat(10));
                        records.forEach(rec -> log.info("{} occurred {} times.", rec.key(), rec.value()));
                        log.info("=".repeat(10));
                        consumer.commitSync();
                    }
                    sleep();
                }
            }
        });
    }

    void startStreaming(Topology topology) {
        try (var streams = new KafkaStreams(topology, config)) {
            streams.start();
            log.info("Started Streaming.");
            log.trace(topology.describe().toString());

            while (!isShuttingDown.get()) {
                sleep();
            }
        }
    }

    @SneakyThrows
    List<String> loadRainbowColours() {
        return Resources.readLines(Resources.getResource("rainbow-colours.txt"), StandardCharsets.UTF_8);
    }

    @SneakyThrows
    List<String> loadUsers() {
        return Resources.readLines(Resources.getResource("users.txt"), StandardCharsets.UTF_8);
    }

    @SneakyThrows
    void sleep() {
        TimeUnit.MILLISECONDS.sleep(100);
    }
}
