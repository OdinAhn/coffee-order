package com.example.coffee.analytics.consumer;

import com.example.coffee.analytics.service.AnalyticsService;
import com.example.coffee.order.dto.OrderEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class AnalyticsConsumer {
    private final AnalyticsService analytics;

    public AnalyticsConsumer(AnalyticsService analytics) {
        this.analytics = analytics;
    }

    @KafkaListener(topics = "${orders.topic}", groupId = "coffee-analytics", concurrency = "3")
    public void collect(OrderEvent event) {
        analytics.collect(event);
    }
}
