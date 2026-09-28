package com.example.coffee.menu.service;

import com.example.coffee.order.service.OrderEventSender;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:coffee_cache;MODE=MySQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1",
        "spring.cache.type=redis",
        "spring.data.redis.host=127.0.0.1",
        "spring.data.redis.port=63999"
})
class MenuCacheFailureTest {
    @Autowired MenuService menus;
    @MockitoBean OrderEventSender sender;

    @Test
    void menusRemainAvailableWhenRedisIsDown() {
        assertThat(menus.list()).hasSize(4);
    }
}
