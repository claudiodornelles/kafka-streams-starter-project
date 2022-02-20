package com.claudiodornelles.kafka.streams.consumers;

import com.claudiodornelles.kafka.streams.FavoriteColourApp;
import com.claudiodornelles.kafka.streams.config.KafkaConfig;
import com.claudiodornelles.kafka.streams.producers.FavoriteColourProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;


public class Consumers {

    private static final Logger LOGGER = LoggerFactory.getLogger(Consumers.class);

    public static void main(String[] args) {
        consume(FavoriteColourProducer.TOPIC, StringDeserializer.class, StringDeserializer.class, "firstConsumer");
        consume(FavoriteColourApp.INTERMEDIARY_TOPIC, StringDeserializer.class, StringDeserializer.class, "secondConsumer");
        consume(FavoriteColourApp.OUTPUT_TOPIC, StringDeserializer.class, LongDeserializer.class, "thirdConsumer");
    }

    private static void consume(String topic, Class keyDeserializer, Class valueDeserializer, String groupId) {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(KafkaConfig.consumerProperties(keyDeserializer, valueDeserializer, groupId))) {
            ConsumerRecords<String, String> messages;
            consumer.subscribe(Collections.singleton(topic));

            messages = consumer.poll(Duration.ofMillis(10000));
            if (!messages.isEmpty()) {
                for (ConsumerRecord<String, String> message : messages) {
                    LOGGER.info("Message consumed from Topic {{}} | Key {{}} | Value {{}} | Partition {{}} | Offset {{}}",
                            message.topic(),
                            message.key(),
                            message.value(),
                            message.partition(),
                            message.offset());
                }
            }
        }
    }

}
