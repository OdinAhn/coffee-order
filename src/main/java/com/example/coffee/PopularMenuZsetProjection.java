package com.example.coffee;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "popularity.zset.enabled", havingValue = "true", matchIfMissing = true)
public class PopularMenuZsetProjection {
    private static final Logger log = LoggerFactory.getLogger(PopularMenuZsetProjection.class);
    static final String KEY = "popular:7d:counts";

    private final JdbcTemplate jdbc;
    private final StringRedisTemplate redis;
    private final Clock clock;

    public PopularMenuZsetProjection(JdbcTemplate jdbc, StringRedisTemplate redis, Clock clock) {
        this.jdbc = jdbc;
        this.redis = redis;
        this.clock = clock;
    }

    @Scheduled(fixedDelayString = "${popularity.zset.refresh-ms:10000}")
    public void refresh() {
        Instant now = clock.instant();
        List<MenuCount> counts = jdbc.query(
                "SELECT menu_id, COUNT(*) AS order_count FROM orders " +
                        "WHERE ordered_at >= ? AND ordered_at <= ? GROUP BY menu_id",
                (rs, rowNum) -> new MenuCount(rs.getLong("menu_id"), rs.getLong("order_count")),
                Timestamp.from(now.minus(7, ChronoUnit.DAYS)), Timestamp.from(now));
        try {
            if (counts.isEmpty()) {
                redis.delete(KEY);
                return;
            }
            String temporaryKey = KEY + ":building:" + UUID.randomUUID();
            for (MenuCount count : counts) {
                redis.opsForZSet().add(temporaryKey, Long.toString(count.menuId()), count.orderCount());
            }
            redis.expire(temporaryKey, Duration.ofMinutes(2));
            redis.rename(temporaryKey, KEY);
        } catch (RuntimeException exception) {
            log.warn("Popularity ZSET refresh failed: {}", exception.toString());
        }
    }

    private record MenuCount(long menuId, long orderCount) {}
}
