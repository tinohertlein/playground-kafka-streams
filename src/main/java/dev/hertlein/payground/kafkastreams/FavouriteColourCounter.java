package dev.hertlein.payground.kafkastreams;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Arrays.asList;


@Slf4j
public class FavouriteColourCounter extends StreamingApp {

    static final String TOPIC_INPUT = "favourite-colours-input";
    static final String TOPIC_OUTPUT = "favourite-colour-count-output";

    FavouriteColourCounter() {
        super(config(), TOPIC_INPUT, TOPIC_OUTPUT);
    }

    @SneakyThrows
    public static void main(String[] args) {
        new FavouriteColourCounter().run();
    }

    private static Properties config() {
        var config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-colour-counter");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5_000);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "favouritecolourcount-output-consumer");

        return config;
    }

    @SneakyThrows
    @Override
    void startProducingThread(Properties config, String topic, AtomicBoolean isShuttingDown) {
        Thread.startVirtualThread(
                () -> {
                    var colours = loadRainbowColours();
                    var users = loadUsers();
                    var random = new Random();

                    try (var producer = new KafkaProducer<>(config)) {
                        log.info("Started producing to {}.", asList(topic));
                        while (!isShuttingDown.get()) {
                            var aColour = colours.get(random.nextInt(colours.size()));
                            var aUser = users.get(random.nextInt(users.size()));
                            producer.send(new ProducerRecord<>(topic, String.format("%s:%s", aUser, aColour)));
                            producer.flush();
                            sleep();
                        }
                    }
                });
    }

    @Override
    Topology topology() {
        StreamsBuilder builder = new StreamsBuilder();

        builder.<String, String>stream(TOPIC_INPUT)
                .filter((key, value) -> value.contains(":"))
                .selectKey((key, value) -> value.split(":")[0], Named.as("user-as-key"))
                .mapValues((value) -> value.split(":")[1], Named.as("colour-as-value"))
                .toTable()
                .groupBy((user, colour) -> new KeyValue<>(colour, colour))
                .count(Materialized.as("count"))
                .toStream()
                .to(TOPIC_OUTPUT, Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build();
    }
}