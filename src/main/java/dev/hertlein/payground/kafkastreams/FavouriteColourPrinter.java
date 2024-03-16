package dev.hertlein.payground.kafkastreams;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;


@Slf4j
public class FavouriteColourPrinter extends StreamingApp {

    static final String TOPIC_INPUT_USERS = "users-print-input";
    static final String TOPIC_INPUT_COLOURS = "colours-print-input";
    static final String TOPIC_OUTPUT = "favourite-colour-print-output";

    private final Random random = new Random();

    FavouriteColourPrinter() {
        super(config(), List.of(TOPIC_INPUT_USERS, TOPIC_INPUT_COLOURS), TOPIC_OUTPUT);
    }

    @SneakyThrows
    public static void main(String[] args) {
        new FavouriteColourPrinter().run();
    }

    private static Properties config() {
        var config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-colour-printer");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5_000);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "favouritecolourprint-output-consumer");

        return config;
    }

    @SneakyThrows
    @Override
    void startProducingThread(Properties config, List<String> topics, AtomicBoolean isShuttingDown) {

        record User(String heroName, String realName) {

            static User fromString(String s) {
                var split = s.split(":");
                return new User(split[0], split[1]);
            }
        }

        Thread.startVirtualThread(
                () -> {
                    var users = loadUsers()
                            .stream()
                            .map(User::fromString)
                            .toList();
                    var colours = loadRainbowColours();

                    try (var producer = new KafkaProducer<>(config)) {
                        users.forEach(user ->
                                producer.send(new ProducerRecord<>(topics.getFirst(), user.heroName, user.realName))
                        );
                        log.info("Produced all users to {}.", topics.getFirst());

                        log.info("Started producing colours to {}.", topics.getLast());
                        while (!isShuttingDown.get()) {
                            var aColour = colours.get(random.nextInt(colours.size()));
                            var superHero = users.get(random.nextInt(users.size()));
                            producer.send(new ProducerRecord<>(topics.getLast(), superHero.heroName, aColour));
                            producer.flush();
                            sleep();
                        }
                    }
                });
    }

    @Override
    void startConsumingThread() {
        Thread.startVirtualThread(() ->
        {
            try (var consumer = new KafkaConsumer<Integer, String>(config)) {
                consumer.subscribe(List.of(outputTopic));
                log.info("Started Consuming from {}.", List.of(outputTopic));

                while (!isShuttingDown.get()) {
                    var records = consumer.poll(Duration.of(1, ChronoUnit.SECONDS));
                    if (!records.isEmpty()) {
                        records.forEach(rec -> log.info("{} alias {}", rec.key(), rec.value()));
                        consumer.commitSync();
                    }
                    sleep();
                }
            }
        });
    }

    @Override
    Topology topology() {
        StreamsBuilder builder = new StreamsBuilder();

        GlobalKTable<String, String> superheroes = builder.globalTable(TOPIC_INPUT_USERS);
        KStream<String, String> colours = builder.stream(TOPIC_INPUT_COLOURS);

        colours.join(superheroes,
                (key, value) -> key,
                (colour, superheroRealName) -> String.format("%s now likes '%s' the most", superheroRealName, colour)
        ).to(TOPIC_OUTPUT);

        return builder.build();
    }
}