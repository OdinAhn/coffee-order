package com.example.coffee;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaOrderEventSender implements OrderEventSender {
    private final KafkaTemplate<String, OrderEvent> kafka;
    private final String topic;

    public KafkaOrderEventSender(KafkaTemplate<String, OrderEvent> kafka,
                                 @Value("${orders.topic}") String topic) {
        this.kafka = kafka;
        this.topic = topic;
    }

    @Override
    public void send(OrderEvent event) {
        try {
            kafka.send(topic, Long.toString(event.userId()), event).get(5, TimeUnit.SECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Kafka publication interrupted", exception);
        } catch (ExecutionException | TimeoutException exception) {
            throw new IllegalStateException("Kafka publication failed", exception);
        }
    }
}
