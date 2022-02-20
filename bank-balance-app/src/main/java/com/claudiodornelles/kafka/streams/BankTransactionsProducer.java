package com.claudiodornelles.kafka.streams;

import com.claudiodornelles.kafka.streams.model.Transaction;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class BankTransactionsProducer {

    public static final String INPUT_TOPIC = "bank-transactions";
    private static final List<String> CUSTOMERS = new ArrayList<>();
    private static final Random RANDOM = new Random();
    private static final Logger LOGGER = LoggerFactory.getLogger(BankTransactionsProducer.class);

    static {
        CUSTOMERS.add("John");
        CUSTOMERS.add("Alice");
        CUSTOMERS.add("Maria");
        CUSTOMERS.add("Marc");
        CUSTOMERS.add("Richard");
        CUSTOMERS.add("Anne");
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProperties.put(ProducerConfig.RETRIES_CONFIG, "3");
        producerProperties.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        producerProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        try (Producer<String, String> producer = new KafkaProducer<>(producerProperties)) {
            int i = 0;
            while (true) {
                try {
                    for (String customerName : CUSTOMERS) {
                        producer.send(newRandomTransaction(customerName), (metadata, exception) -> {
                            if (exception != null) {
                                LOGGER.error("Could not send the message over the Topic {{}}", metadata.topic(), exception);
                            }
                            LOGGER.info("New message sent to Topic {{}} | Partition {{}} | Offset {{}}",
                                    metadata.topic(),
                                    metadata.partition(),
                                    metadata.offset());
                        });
                        Thread.sleep(2000);
                    }
                    i++;
                    if (i == 999999) {
                        throw new InterruptedException();
                    }
                } catch (InterruptedException e) {
                    LOGGER.error("Something went wrong: {}", e.getMessage(), e);
                    break;
                }
            }
        }
    }

    private static ProducerRecord<String, String> newRandomTransaction(String name) {
        int upperbound = 100;
        Transaction transaction = new Transaction();
        transaction
                .name(name)
                .amount(BigDecimal.valueOf(RANDOM.nextInt(upperbound)));
        return new ProducerRecord<>(INPUT_TOPIC, name, transaction.toString());
    }
}
