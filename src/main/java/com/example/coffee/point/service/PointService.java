package com.example.coffee.point.service;

import com.example.coffee.common.error.ApiException;
import com.example.coffee.point.dto.ChargeRequest;
import com.example.coffee.point.dto.ChargeResponse;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class PointService {
    private final JdbcTemplate jdbc;

    public PointService(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Transactional
    public ChargeResponse charge(ChargeRequest request) {
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
        return ChargeResponse.builder().userId(request.userId()).balance(balance).build();
    }
}
