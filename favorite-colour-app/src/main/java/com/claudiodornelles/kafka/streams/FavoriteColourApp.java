package com.claudiodornelles.kafka.streams;

import com.claudiodornelles.kafka.streams.config.KafkaConfig;
import com.claudiodornelles.kafka.streams.producers.FavoriteColourProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class FavoriteColourApp {

    private static final String APPLICATION_ID = "favorite-colour-application";
    public static final String INTERMEDIARY_TOPIC = "favorite-colour-data-manipulation-topic";
    public static final String OUTPUT_TOPIC = "favorite-colour-output";
    private static final List<String> VALID_COLOURS = new ArrayList<>();


    static {
        VALID_COLOURS.add("blue");
        VALID_COLOURS.add("green");
        VALID_COLOURS.add("red");
    }

    public static void main(String[] args) {

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> messageInput = builder.stream(FavoriteColourProducer.TOPIC);

        KStream<String, String> usersAndColours = messageInput
                .filter((key, value) -> value.contains(","))
                .selectKey((key, value) -> value.split(",")[0].toLowerCase())
                .mapValues((key, value) -> value.split(",")[1].toLowerCase())
                .filter((user, favoriteColour) -> VALID_COLOURS.contains(favoriteColour));

        usersAndColours.to(INTERMEDIARY_TOPIC);

        KTable<String, String> usersAndColoursTable = builder.table(INTERMEDIARY_TOPIC);

        KTable<String, Long> colourCount = usersAndColoursTable
                .groupBy((user, colour) -> KeyValue.pair(colour, colour))
                .count(Materialized.as("count"));

        colourCount.toStream().to(OUTPUT_TOPIC);

        KafkaStreams stream = new KafkaStreams(builder.build(), defaultProperties());
        stream.start();
        Runtime.getRuntime().addShutdownHook(new Thread(stream::close));

    }

    private static Properties defaultProperties() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.KAFKA_SERVER);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return properties;
    }
}
