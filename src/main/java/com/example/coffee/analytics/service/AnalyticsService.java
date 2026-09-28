package com.example.coffee.analytics.service;

import com.example.coffee.order.dto.OrderEvent;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class AnalyticsService {
    private final JdbcTemplate jdbc;

    public AnalyticsService(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Transactional
    public void collect(OrderEvent event) {
        jdbc.update("INSERT INTO collected_order_events(event_id, order_id, user_id, menu_id, paid_amount) " +
                        "VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE event_id = event_id",
                event.eventId(), event.orderId(), event.userId(), event.menuId(), event.paidAmount());
    }
}
