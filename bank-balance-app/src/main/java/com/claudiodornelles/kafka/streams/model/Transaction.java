package com.claudiodornelles.kafka.streams.model;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;

public class Transaction {

    private String name;
    private BigDecimal amount;
    private Timestamp time;

    public Transaction() {
        this.time = Timestamp.from(Instant.now());
    }

    public Transaction(String name, BigDecimal amount, Timestamp time) {
        this.name = name;
        this.amount = amount;
        this.time = time;
    }

    public Transaction(String name, String amount, String time) {
        this.name = name;
        this.amount = BigDecimal.valueOf(Long.parseLong(amount));
        this.time = Timestamp.valueOf(time);
    }

    public String getName() {
        return name;
    }

    public Transaction name(String name) {
        this.name = name;
        return this;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public Transaction amount(BigDecimal amount) {
        this.amount = amount;
        return this;
    }

    public Timestamp getTime() {
        return time;
    }

    public Transaction time(Timestamp time) {
        this.time = time;
        return this;
    }

    @Override
    public String toString() {
        return "{" +
                "\"name\":" + "\"" + name + "\"" +
                ",\"amount\":" + amount +
                ",\"time\":" + "\"" + time + "\"" +
                '}';
    }
}
