package com.example.coffee.menu.service;

import com.example.coffee.menu.dto.PopularMenu;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class PopularMenuService {
    private final JdbcTemplate jdbc;
    private final Clock clock;

    public PopularMenuService(JdbcTemplate jdbc, Clock clock) {
        this.jdbc = jdbc;
        this.clock = clock;
    }

    public List<PopularMenu> list() {
        Instant now = clock.instant();
        return jdbc.query("SELECT m.id, m.name, m.price, COUNT(o.id) AS order_count " +
                        "FROM orders o JOIN menus m ON m.id = o.menu_id " +
                        "WHERE o.ordered_at >= ? AND o.ordered_at <= ? " +
                        "GROUP BY m.id, m.name, m.price " +
                        "ORDER BY order_count DESC, m.id ASC LIMIT 3",
                (rs, rowNum) -> PopularMenu.builder().menuId(rs.getLong("id"))
                        .name(rs.getString("name")).price(rs.getLong("price"))
                        .orderCount(rs.getLong("order_count")).build(),
                Timestamp.from(now.minus(7, ChronoUnit.DAYS)), Timestamp.from(now));
    }
}
