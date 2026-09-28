package com.example.coffee;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.sql.Timestamp;
import lombok.Builder;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/menus/popular")
public class PopularMenuController {
    private final JdbcTemplate jdbc;
    private final Clock clock;

    public PopularMenuController(JdbcTemplate jdbc, Clock clock) {
        this.jdbc = jdbc;
        this.clock = clock;
    }

    @GetMapping
    public ResponseEntity<List<PopularMenu>> list() {
        Instant now = clock.instant();
        List<PopularMenu> menus = jdbc.query("SELECT m.id, m.name, m.price, COUNT(o.id) AS order_count " +
                        "FROM orders o JOIN menus m ON m.id = o.menu_id " +
                        "WHERE o.ordered_at >= ? AND o.ordered_at <= ? " +
                        "GROUP BY m.id, m.name, m.price " +
                        "ORDER BY order_count DESC, m.id ASC LIMIT 3",
                (rs, rowNum) -> PopularMenu.builder().menuId(rs.getLong("id"))
                        .name(rs.getString("name")).price(rs.getLong("price"))
                        .orderCount(rs.getLong("order_count")).build(),
                Timestamp.from(now.minus(7, ChronoUnit.DAYS)), Timestamp.from(now));
        return ResponseEntity.ok(menus);
    }

    @Builder
    public record PopularMenu(long menuId, String name, long price, long orderCount) {}
}
