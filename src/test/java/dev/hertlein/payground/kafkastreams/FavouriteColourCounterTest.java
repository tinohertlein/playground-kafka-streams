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

class FavouriteColourCounterTest {

    private TopologyTestDriver testDriver;

    @BeforeEach
    void beforeEach() {
        var config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-colour-counter-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "server:1111");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        testDriver = new TopologyTestDriver(new FavouriteColourCounter().topology(), config);
    }

    @AfterEach
    void afterEach() {
        testDriver.close();
    }

    @Test
    void should_count_occurrences() {
        var inputTopic = testDriver.createInputTopic(FavouriteColourCounter.TOPIC_INPUT, Serdes.String().serializer(), Serdes.String().serializer());
        var outputTopic = testDriver.createOutputTopic(FavouriteColourCounter.TOPIC_OUTPUT, Serdes.String().deserializer(), Serdes.Long().deserializer());
        assertThat(outputTopic.isEmpty()).isTrue();

        inputTopic.pipeInput("a-key", "user1:green");
        inputTopic.pipeInput("a-key", "user2:yellow");
        assertThat(outputTopic.readKeyValuesToList()).containsExactlyInAnyOrder(new KeyValue<>("green", 1L), new KeyValue<>("yellow", 1L));

        inputTopic.pipeInput("a-key", "user1:red");
        inputTopic.pipeInput("a-key", "user3:yellow");
        assertThat(outputTopic.readKeyValuesToList()).containsExactlyInAnyOrder(new KeyValue<>("green", 0L), new KeyValue<>("red", 1L), new KeyValue<>("yellow", 2L));
    }
}