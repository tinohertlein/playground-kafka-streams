package dev.hertlein.payground.kafkastreams;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class FavouriteColourPrinterTest {

    private TopologyTestDriver testDriver;

    @BeforeEach
    void beforeEach() {
        var config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-colour-printer-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "server:1111");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        testDriver = new TopologyTestDriver(new FavouriteColourPrinter().topology(), config);
    }

    @AfterEach
    void afterEach() {
        testDriver.close();
    }

    @Test
    void should_print_favourite_colours_of_users() {
        var usersInputTopic = testDriver.createInputTopic(FavouriteColourPrinter.TOPIC_INPUT_USERS, Serdes.String().serializer(), Serdes.String().serializer());
        var coloursInputTopic = testDriver.createInputTopic(FavouriteColourPrinter.TOPIC_INPUT_COLOURS, Serdes.String().serializer(), Serdes.String().serializer());
        var outputTopic = testDriver.createOutputTopic(FavouriteColourPrinter.TOPIC_OUTPUT, Serdes.String().deserializer(), Serdes.String().deserializer());
        assertThat(outputTopic.isEmpty()).isTrue();

        usersInputTopic.pipeInput("a-user-key", "a-user-value");
        coloursInputTopic.pipeInput("a-user-key", "yellow");
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a-user-key", "a-user-value now likes 'yellow' the most"));

        coloursInputTopic.pipeInput("a-user-key", "red");
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a-user-key", "a-user-value now likes 'red' the most"));
    }
}