package com.claudiodornelles.kafka.streams.producers;

import com.claudiodornelles.kafka.streams.config.KafkaConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class FavoriteColourProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(FavoriteColourProducer.class);
    public static final String TOPIC = "favorite-colour-input";
    private static final List<String> DATA = new ArrayList<>();

    static {
        DATA.add("stephane,blue");
        DATA.add("john,green");
        DATA.add("stephane,red");
        DATA.add("alice,red");
    }

    public static void main(String[] args) {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(KafkaConfig.producerProperties(StringSerializer.class, StringSerializer.class))) {
            for (String value : DATA) {
                ProducerRecord<String, String> message = new ProducerRecord<>(FavoriteColourProducer.TOPIC, null, value);
                producer.send(message, FavoriteColourProducer.defaultCallback());
            }
        }
    }

    public static Callback defaultCallback() {
        return (metadata, exception) -> {
            if (exception != null) {
                LOGGER.error("Could not send new data: {}", exception.getMessage(), exception);
                return;
            }
            LOGGER.info("New message sent to Topic {{}} | Partition {{}} | Offset {{}} | Timestamp: {{}}",
                    metadata.topic(),
                    metadata.partition(),
                    metadata.offset(),
                    Timestamp.from(Instant.ofEpochMilli(metadata.timestamp())));
        };
    }

}
