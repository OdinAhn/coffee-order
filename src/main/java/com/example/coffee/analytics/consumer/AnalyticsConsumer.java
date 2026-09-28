package com.example.coffee.analytics.consumer;

import com.example.coffee.order.dto.OrderEvent;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class AnalyticsConsumer {
    private final JdbcTemplate jdbc;

    public AnalyticsConsumer(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @KafkaListener(topics = "${orders.topic}", groupId = "coffee-analytics", concurrency = "3")
    @Transactional
    public void collect(OrderEvent event) {
        jdbc.update("INSERT INTO collected_order_events(event_id, order_id, user_id, menu_id, paid_amount) " +
                        "VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE event_id = event_id",
                event.eventId(), event.orderId(), event.userId(), event.menuId(), event.paidAmount());
    }
}
