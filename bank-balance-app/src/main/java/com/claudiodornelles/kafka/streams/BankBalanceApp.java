package com.claudiodornelles.kafka.streams;

import com.claudiodornelles.kafka.streams.model.Transaction;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;

import java.math.BigDecimal;
import java.util.Properties;

public class BankBalanceApp {

    private static final Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss.SSS").create();
    private static final String USERS_BALANCE = "bank-users-balance";

    public static void main(String[] args) {

        StreamsBuilder builder = new StreamsBuilder();

        Properties streamsProperties = new Properties();
        streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance-application");
        streamsProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        streamsProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsProperties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        KStream<String, String> transactions = builder
                .stream(BankTransactionsProducer.INPUT_TOPIC);

        Transaction initialBalance = new Transaction().amount(BigDecimal.ZERO);

        KTable<String, String> usersBalanceTable = transactions
                .groupByKey()
                .aggregate(
                        initialBalance::toString,
                        (key, newTransaction, currentTransaction) -> newBalance(gson.fromJson(newTransaction, Transaction.class), gson.fromJson(currentTransaction, Transaction.class)),
                        Materialized.as("aggregated-stream-store"));

        usersBalanceTable.toStream().to(USERS_BALANCE);

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsProperties);

        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    private static String newBalance(Transaction newTransaction, Transaction currentTransaction) {
        return gson.toJson(new Transaction()
                .amount(currentTransaction.getAmount().add(newTransaction.getAmount()))
                .time(newTransaction.getTime()));
    }


}
