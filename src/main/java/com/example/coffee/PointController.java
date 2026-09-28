package com.example.coffee;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import lombok.Builder;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/points")
public class PointController {
    private final JdbcTemplate jdbc;

    public PointController(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @PostMapping("/charges")
    @Transactional
    public ResponseEntity<ChargeResponse> charge(@Valid @RequestBody ChargeRequest request) {
        jdbc.update("INSERT INTO point_accounts(user_id, balance) VALUES (?, 0) " +
                        "ON DUPLICATE KEY UPDATE user_id = user_id",
                request.userId());
        int updated = jdbc.update("UPDATE point_accounts SET balance = balance + ? " +
                        "WHERE user_id = ? AND balance <= 1000000000000 - ?",
                request.amount(), request.userId(), request.amount());
        if (updated == 0) {
            throw new ApiException(HttpStatus.CONFLICT, "POINT_LIMIT_EXCEEDED", "Point balance limit exceeded");
        }
        Long balance = jdbc.queryForObject("SELECT balance FROM point_accounts WHERE user_id = ?",
                Long.class, request.userId());
        return ResponseEntity.ok(ChargeResponse.builder()
                .userId(request.userId()).balance(balance).build());
    }

    @Builder
    public record ChargeRequest(@Min(1) long userId, @Min(1) @Max(1000000000000L) long amount) {}
    @Builder
    public record ChargeResponse(long userId, long balance) {}
}
