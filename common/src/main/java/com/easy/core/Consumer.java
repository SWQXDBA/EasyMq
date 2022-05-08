package com.easy.core;

import java.time.LocalDateTime;
import java.util.Objects;

public class Consumer extends Client {

    static int passedTimeSecond = 30;
    ConsumerGroup group;

    public LocalDateTime lastResponseTime;
    public Boolean isAlive() {
        final LocalDateTime now = LocalDateTime.now();
        final LocalDateTime expirationTime = lastResponseTime.plusSeconds(passedTimeSecond);
        return now.isBefore(expirationTime);
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Consumer consumer = (Consumer) o;
        return Objects.equals(name, consumer.name) && Objects.equals(ip, consumer.ip) && Objects.equals(port, consumer.port);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, ip, port);
    }
}
