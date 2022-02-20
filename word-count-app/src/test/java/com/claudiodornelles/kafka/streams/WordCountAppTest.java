package com.claudiodornelles.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

class WordCountAppTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, Long> outputTopic;

    @BeforeEach
    void beforeEach() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        WordCountApp wordCountApp = new WordCountApp();
        Topology topology = wordCountApp.getTopology();

        testDriver = new TopologyTestDriver(topology, config);

        inputTopic = testDriver.createInputTopic("word-count-input", Serdes.String().serializer(), Serdes.String().serializer());
        outputTopic = testDriver.createOutputTopic("word-count-output", Serdes.String().deserializer(), Serdes.Long().deserializer());
    }

    @AfterEach
    void afterEach() {
        testDriver.close();
    }

    @Test
    void shouldCountSingleRecord() {
        produceRecord("testing kafka streams with a kafka streams message");
        List<KeyValue<String, Long>> outputRecords = getKeyValuesOutput();
        Assertions.assertNotNull(outputRecords);
        Assertions.assertFalse(outputRecords.isEmpty());
        Assertions.assertEquals(8, outputRecords.size());
        Assertions.assertTrue(outputRecords.contains(new KeyValue<>("testing", 1L)));
        Assertions.assertTrue(outputRecords.contains(new KeyValue<>("kafka", 2L)));
        Assertions.assertTrue(outputRecords.contains(new KeyValue<>("streams", 2L)));
        Assertions.assertTrue(outputRecords.contains(new KeyValue<>("with", 1L)));
        Assertions.assertTrue(outputRecords.contains(new KeyValue<>("a", 1L)));
        Assertions.assertTrue(outputRecords.contains(new KeyValue<>("message", 1L)));
    }

    @Test
    void shouldCountTwoRecords() {
        produceRecord("testing kafka streams with a kafka streams message");
        produceRecord("testing kafka streams with a duplicated kafka streams message");
        List<KeyValue<String, Long>> outputRecords = getKeyValuesOutput();
        System.out.println("outputRecords = " + outputRecords);
        Assertions.assertNotNull(outputRecords);
        Assertions.assertFalse(outputRecords.isEmpty());
        Assertions.assertEquals(17, outputRecords.size());
        Assertions.assertTrue(outputRecords.contains(new KeyValue<>("testing", 2L)));
        Assertions.assertTrue(outputRecords.contains(new KeyValue<>("kafka", 4L)));
        Assertions.assertTrue(outputRecords.contains(new KeyValue<>("streams", 4L)));
        Assertions.assertTrue(outputRecords.contains(new KeyValue<>("with", 2L)));
        Assertions.assertTrue(outputRecords.contains(new KeyValue<>("a", 2L)));
        Assertions.assertTrue(outputRecords.contains(new KeyValue<>("duplicated", 1L)));
        Assertions.assertTrue(outputRecords.contains(new KeyValue<>("message", 2L)));
    }

    @Test
    void shouldCountWordsIgnoringCase() {
        produceRecord("TeStiNg KAFKA sTReAms with a Kafka Streams message");
        produceRecord("testing kafka streams With a dUplicated kafka streams message");
        List<KeyValue<String, Long>> outputRecords = getKeyValuesOutput();
        System.out.println("outputRecords = " + outputRecords);
        Assertions.assertNotNull(outputRecords);
        Assertions.assertFalse(outputRecords.isEmpty());
        Assertions.assertEquals(17, outputRecords.size());
        Assertions.assertTrue(outputRecords.contains(new KeyValue<>("testing", 2L)));
        Assertions.assertTrue(outputRecords.contains(new KeyValue<>("kafka", 4L)));
        Assertions.assertTrue(outputRecords.contains(new KeyValue<>("streams", 4L)));
        Assertions.assertTrue(outputRecords.contains(new KeyValue<>("with", 2L)));
        Assertions.assertTrue(outputRecords.contains(new KeyValue<>("a", 2L)));
        Assertions.assertTrue(outputRecords.contains(new KeyValue<>("duplicated", 1L)));
        Assertions.assertTrue(outputRecords.contains(new KeyValue<>("message", 2L)));
    }

    private void produceRecord(String message) {
        inputTopic.pipeInput(message);
    }

    private List<KeyValue<String, Long>> getKeyValuesOutput() {
        return outputTopic.readKeyValuesToList();
    }

}