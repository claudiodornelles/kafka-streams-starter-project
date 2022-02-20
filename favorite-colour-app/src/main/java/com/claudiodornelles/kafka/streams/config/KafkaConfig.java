package com.claudiodornelles.kafka.streams.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public final class KafkaConfig {

    public static final String KAFKA_SERVER = "127.0.0.1:9092";

    private KafkaConfig() {
    }

    public static Properties producerProperties(Class keySerializer, Class valueSerializer) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        return properties;
    }

    public static Properties consumerProperties(Class keyDeserializer, Class valueDeserializer, String groupId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return properties;
    }

}
