package com.example.coffee;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OutboxPublisher {
    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final JdbcTemplate jdbc;
    private final OrderEventSender sender;
    private final Clock clock;

    public OutboxPublisher(JdbcTemplate jdbc, OrderEventSender sender, Clock clock) {
        this.jdbc = jdbc;
        this.sender = sender;
        this.clock = clock;
    }

    @Transactional
    public boolean publishOne() {
        Instant now = clock.instant();
        List<OrderEventSender.OrderEvent> ready = jdbc.query(
                "SELECT id, order_id, user_id, menu_id, paid_amount FROM order_outbox " +
                        "WHERE next_attempt_at <= ? ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED",
                (rs, rowNum) -> OrderEventSender.OrderEvent.builder()
                        .eventId(rs.getLong("id")).orderId(rs.getLong("order_id"))
                        .userId(rs.getLong("user_id")).menuId(rs.getLong("menu_id"))
                        .paidAmount(rs.getLong("paid_amount")).build(),
                Timestamp.from(now));
        if (ready.isEmpty()) {
            return false;
        }

        OrderEventSender.OrderEvent event = ready.get(0);
        try {
            sender.send(event);
            jdbc.update("DELETE FROM order_outbox WHERE id = ?", event.eventId());
        } catch (RuntimeException exception) {
            jdbc.update("UPDATE order_outbox SET attempts = attempts + 1, next_attempt_at = ? WHERE id = ?",
                    Timestamp.from(now.plus(Duration.ofSeconds(5))), event.eventId());
            log.warn("Could not publish order event {}: {}", event.eventId(), exception.toString());
        }
        return true;
    }
}
