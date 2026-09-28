package com.example.coffee;

import java.util.List;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class MenuService {
    private final JdbcTemplate jdbc;

    public MenuService(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Cacheable(cacheNames = "menus", key = "'all'")
    public List<Menu> list() {
        return jdbc.query("SELECT id, name, price FROM menus ORDER BY id",
                (rs, rowNum) -> Menu.builder().id(rs.getLong("id"))
                        .name(rs.getString("name")).price(rs.getLong("price")).build());
    }
}
