package com.example.coffee;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import lombok.Builder;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/orders")
public class OrderController {
    private final JdbcTemplate jdbc;
    private final Clock clock;

    public OrderController(JdbcTemplate jdbc, Clock clock) {
        this.jdbc = jdbc;
        this.clock = clock;
    }

    @PostMapping
    @Transactional
    public ResponseEntity<OrderResponse> place(@Valid @RequestBody OrderRequest request,
                                               @RequestHeader("Idempotency-Key") String requestKey) {
        if (requestKey.isBlank() || requestKey.length() > 128) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "INVALID_REQUEST",
                    "Idempotency-Key must contain 1 to 128 characters");
        }
        Long price = jdbc.query("SELECT price FROM menus WHERE id = ?",
                rs -> rs.next() ? rs.getLong(1) : null, request.menuId());
        if (price == null) {
            throw new ApiException(HttpStatus.NOT_FOUND, "MENU_NOT_FOUND", "Menu does not exist");
        }
        Long balance = jdbc.query("SELECT balance FROM point_accounts WHERE user_id = ? FOR UPDATE",
                rs -> rs.next() ? rs.getLong(1) : null, request.userId());
        if (balance == null) {
            throw new ApiException(HttpStatus.CONFLICT, "INSUFFICIENT_POINTS", "Insufficient points");
        }
        OrderResponse previous = jdbc.query(
                "SELECT id, menu_id, paid_amount, ordered_at FROM orders WHERE user_id = ? AND request_key = ? FOR UPDATE",
                rs -> rs.next() ? OrderResponse.builder()
                        .orderId(rs.getLong("id")).userId(request.userId())
                        .menuId(rs.getLong("menu_id")).paidAmount(rs.getLong("paid_amount"))
                        .orderedAt(rs.getTimestamp("ordered_at").toInstant()).build() : null,
                request.userId(), requestKey);
        if (previous != null) {
            if (previous.menuId() != request.menuId()) {
                throw new ApiException(HttpStatus.CONFLICT, "IDEMPOTENCY_KEY_REUSED",
                        "Idempotency-Key was already used for another menu");
            }
            return ResponseEntity.status(HttpStatus.CREATED).body(previous);
        }
        int debited = jdbc.update("UPDATE point_accounts SET balance = balance - ? " +
                        "WHERE user_id = ? AND balance >= ?", price, request.userId(), price);
        if (debited == 0) {
            throw new ApiException(HttpStatus.CONFLICT, "INSUFFICIENT_POINTS", "Insufficient points");
        }

        Instant now = clock.instant();
        KeyHolder key = new GeneratedKeyHolder();
        jdbc.update(connection -> {
            PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO orders(user_id, menu_id, paid_amount, ordered_at, request_key) VALUES (?, ?, ?, ?, ?)",
                    Statement.RETURN_GENERATED_KEYS);
            statement.setLong(1, request.userId());
            statement.setLong(2, request.menuId());
            statement.setLong(3, price);
            statement.setTimestamp(4, Timestamp.from(now));
            statement.setString(5, requestKey);
            return statement;
        }, key);
        long orderId = key.getKey().longValue();
        jdbc.update("INSERT INTO order_outbox(order_id, user_id, menu_id, paid_amount, next_attempt_at) " +
                "VALUES (?, ?, ?, ?, ?)", orderId, request.userId(), request.menuId(), price, Timestamp.from(now));
        return ResponseEntity.status(HttpStatus.CREATED).body(OrderResponse.builder()
                .orderId(orderId).userId(request.userId()).menuId(request.menuId())
                .paidAmount(price).orderedAt(now).build());
    }

    @Builder
    public record OrderRequest(@Min(1) long userId, @Min(1) long menuId) {}
    @Builder
    public record OrderResponse(long orderId, long userId, long menuId, long paidAmount, Instant orderedAt) {}
}