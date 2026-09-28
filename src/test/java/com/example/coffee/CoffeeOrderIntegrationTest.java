package com.example.coffee;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest
@AutoConfigureMockMvc
class CoffeeOrderIntegrationTest {
    private static final Instant NOW = Instant.parse("2026-09-28T00:00:00Z");

    @Autowired JdbcTemplate jdbc;
    @Autowired PointController points;
    @Autowired OrderController orders;
    @Autowired PopularMenuController popular;
    @Autowired OutboxPublisher publisher;
    @Autowired AnalyticsConsumer analytics;
    @Autowired MockMvc mvc;
    @MockitoBean OrderEventSender sender;

    @TestConfiguration
    static class FixedClockConfig {
        @Bean
        @Primary
        Clock testClock() {
            return Clock.fixed(NOW, ZoneOffset.UTC);
        }
    }

    @BeforeEach
    void clearData() {
        jdbc.update("DELETE FROM collected_order_events");
        jdbc.update("DELETE FROM order_outbox");
        jdbc.update("DELETE FROM orders");
        jdbc.update("DELETE FROM point_accounts");
    }

    @Test
    void chargeOrderAndPublishAreConsistent() {
        assertThat(points.charge(PointController.ChargeRequest.builder()
                .userId(1).amount(5000).build()).getBody().balance()).isEqualTo(5000);

        OrderController.OrderResponse order = orders.place(OrderController.OrderRequest.builder()
                .userId(1).menuId(1).build(), "charge-order-1").getBody();
        assertThat(order.paidAmount()).isEqualTo(4500);
        assertThat(balance(1)).isEqualTo(500);
        assertThat(popular.list().getBody()).extracting(PopularMenuController.PopularMenu::orderCount)
                .containsExactly(1L);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM order_outbox", Long.class)).isEqualTo(1);

        long eventId = jdbc.queryForObject("SELECT id FROM order_outbox", Long.class);
        assertThat(publisher.publishOne()).isTrue();
        verify(sender).send(new OrderEventSender.OrderEvent(eventId, order.orderId(), 1, 1, 4500));
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM order_outbox", Long.class)).isZero();
    }

    @Test
    void menuApiReturnsSeededMenus() throws Exception {
        mvc.perform(get("/api/menus"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(4))
                .andExpect(jsonPath("$[0].name").value("Americano"));
    }

    @Test
    void concurrentOrdersCannotOverspend() throws Exception {
        points.charge(PointController.ChargeRequest.builder().userId(2).amount(45000).build());
        AtomicInteger requestNumber = new AtomicInteger();
        int succeeded = runConcurrently(20, () -> {
            try {
                orders.place(OrderController.OrderRequest.builder().userId(2).menuId(1).build(),
                        "concurrent-order-" + requestNumber.incrementAndGet());
                return true;
            } catch (ApiException exception) {
                assertThat(exception.code()).isEqualTo("INSUFFICIENT_POINTS");
                return false;
            }
        });
        assertThat(succeeded).isEqualTo(10);
        assertThat(balance(2)).isZero();
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM orders WHERE user_id = 2", Long.class))
                .isEqualTo(10);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM order_outbox WHERE user_id = 2", Long.class))
                .isEqualTo(10);
    }

    @Test
    void concurrentChargesDoNotLoseUpdates() throws Exception {
        int succeeded = runConcurrently(20, () -> {
            points.charge(PointController.ChargeRequest.builder().userId(3).amount(100).build());
            return true;
        });
        assertThat(succeeded).isEqualTo(20);
        assertThat(balance(3)).isEqualTo(2000);
    }

    @Test
    void popularMenusUseRollingSevenDaysAndDeterministicTies() {
        points.charge(PointController.ChargeRequest.builder().userId(4).amount(100).build());
        addOrder(4, 1, NOW.minus(Duration.ofDays(7)));
        addOrder(4, 1, NOW);
        addOrder(4, 2, NOW.minus(Duration.ofDays(1)));
        addOrder(4, 2, NOW.minus(Duration.ofDays(2)));
        addOrder(4, 3, NOW.minus(Duration.ofDays(3)));
        addOrder(4, 4, NOW.minus(Duration.ofDays(7)).minusSeconds(1));

        List<PopularMenuController.PopularMenu> result = popular.list().getBody();
        assertThat(result).extracting(PopularMenuController.PopularMenu::menuId)
                .containsExactly(1L, 2L, 3L);
        assertThat(result).extracting(PopularMenuController.PopularMenu::orderCount)
                .containsExactly(2L, 2L, 1L);
    }

    @Test
    void failedDeliveryStaysInOutboxForRetry() {
        points.charge(PointController.ChargeRequest.builder().userId(5).amount(4500).build());
        orders.place(OrderController.OrderRequest.builder().userId(5).menuId(1).build(), "delivery-retry");
        doThrow(new IllegalStateException("platform unavailable")).doNothing().when(sender).send(any());

        assertThat(publisher.publishOne()).isTrue();
        assertThat(jdbc.queryForObject("SELECT attempts FROM order_outbox", Integer.class)).isEqualTo(1);
        assertThat(publisher.publishOne()).isFalse();
        jdbc.update("UPDATE order_outbox SET next_attempt_at = ?", Timestamp.from(NOW));
        assertThat(publisher.publishOne()).isTrue();
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM order_outbox", Long.class)).isZero();
    }

    @Test
    void concurrentPublishersDoNotClaimTheSameEvent() throws Exception {
        points.charge(PointController.ChargeRequest.builder().userId(6).amount(4500).build());
        orders.place(OrderController.OrderRequest.builder().userId(6).menuId(1).build(), "publish-concurrent");
        CountDownLatch sending = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        doAnswer(invocation -> {
            sending.countDown();
            release.await(5, TimeUnit.SECONDS);
            return null;
        }).when(sender).send(any());
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<Boolean> first = pool.submit(publisher::publishOne);
            assertThat(sending.await(5, TimeUnit.SECONDS)).isTrue();
            Future<Boolean> second = pool.submit(publisher::publishOne);
            assertThat(second.get(5, TimeUnit.SECONDS)).isFalse();
            release.countDown();
            assertThat(first.get(5, TimeUnit.SECONDS)).isTrue();
            verify(sender, times(1)).send(any());
        } finally {
            release.countDown();
            pool.shutdownNow();
        }
    }

    @Test
    void invalidRequestsAndInsufficientPointsReturnDefinedErrors() throws Exception {
        mvc.perform(post("/api/points/charges").contentType("application/json")
                        .content("{\"userId\":1,\"amount\":0}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"));
        mvc.perform(post("/api/orders").header("Idempotency-Key", "invalid-request-test")
                        .contentType("application/json")
                        .content("{\"userId\":99,\"menuId\":1}"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("INSUFFICIENT_POINTS"));
        mvc.perform(post("/api/orders").header("Idempotency-Key", "invalid-request-test")
                        .contentType("application/json")
                        .content("{\"userId\":99,\"menuId\":999}"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("MENU_NOT_FOUND"));
    }

    @Test
    void repeatedOrderRequestReturnsOriginalWithoutSecondDebitOrEvent() {
        points.charge(PointController.ChargeRequest.builder().userId(8).amount(4500).build());
        OrderController.OrderRequest request = OrderController.OrderRequest.builder()
                .userId(8).menuId(1).build();

        OrderController.OrderResponse first = orders.place(request, "retry-8").getBody();
        OrderController.OrderResponse replay = orders.place(request, "retry-8").getBody();

        assertThat(replay).isEqualTo(first);
        assertThat(balance(8)).isZero();
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM orders WHERE user_id = 8", Long.class))
                .isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM order_outbox WHERE user_id = 8", Long.class))
                .isEqualTo(1);
    }

    @Test
    void concurrentRetriesCreateOnlyOneOrder() throws Exception {
        points.charge(PointController.ChargeRequest.builder().userId(9).amount(4500).build());
        OrderController.OrderRequest request = OrderController.OrderRequest.builder()
                .userId(9).menuId(1).build();
        ConcurrentLinkedQueue<Long> orderIds = new ConcurrentLinkedQueue<>();

        assertThat(runConcurrently(10, () -> {
            orderIds.add(orders.place(request, "same-key-9").getBody().orderId());
            return true;
        })).isEqualTo(10);
        assertThat(orderIds).hasSize(10).containsOnly(orderIds.peek());
        assertThat(balance(9)).isZero();
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM orders WHERE user_id = 9", Long.class))
                .isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM order_outbox WHERE user_id = 9", Long.class))
                .isEqualTo(1);
    }

    @Test
    void reusedKeyForDifferentMenuConflicts() throws Exception {
        points.charge(PointController.ChargeRequest.builder().userId(10).amount(10000).build());
        orders.place(OrderController.OrderRequest.builder().userId(10).menuId(1).build(), "menu-choice");

        mvc.perform(post("/api/orders").header("Idempotency-Key", "menu-choice")
                        .contentType("application/json")
                        .content("{\"userId\":10,\"menuId\":2}"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("IDEMPOTENCY_KEY_REUSED"));
        assertThat(balance(10)).isEqualTo(5500);
    }

    @Test
    void missingIdempotencyKeyReturnsBadRequest() throws Exception {
        mvc.perform(post("/api/orders").contentType("application/json")
                        .content("{\"userId\":11,\"menuId\":1}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"));
        mvc.perform(post("/api/orders").header("Idempotency-Key", " ")
                        .contentType("application/json")
                        .content("{\"userId\":11,\"menuId\":1}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"));
        mvc.perform(post("/api/orders").header("Idempotency-Key", "x".repeat(129))
                        .contentType("application/json")
                        .content("{\"userId\":11,\"menuId\":1}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"));
    }
    private long balance(long userId) {
        return jdbc.queryForObject("SELECT balance FROM point_accounts WHERE user_id = ?", Long.class, userId);
    }

    private void addOrder(long userId, long menuId, Instant when) {
        jdbc.update("INSERT INTO orders(user_id, menu_id, paid_amount, ordered_at) VALUES (?, ?, ?, ?)",
                userId, menuId, 100, Timestamp.from(when));
    }

    private int runConcurrently(int count, Callable<Boolean> action) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(count);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Boolean>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < count; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    return action.call();
                }));
            }
            start.countDown();
            int succeeded = 0;
            for (Future<Boolean> future : futures) {
                if (future.get()) {
                    succeeded++;
                }
            }
            return succeeded;
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void analyticsConsumerDeduplicatesKafkaRedelivery() {
        OrderEventSender.OrderEvent event = OrderEventSender.OrderEvent.builder()
                .eventId(77).orderId(88).userId(99).menuId(1).paidAmount(4500).build();

        analytics.collect(event);
        analytics.collect(event);

        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM collected_order_events WHERE event_id = 77",
                Long.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT paid_amount FROM collected_order_events WHERE event_id = 77",
                Long.class)).isEqualTo(4500);
    }

    @Test
    @SuppressWarnings("unchecked")
    void zsetProjectionUsesRollingSevenDayCounts() {
        points.charge(PointController.ChargeRequest.builder().userId(7).amount(100).build());
        addOrder(7, 1, NOW);
        addOrder(7, 1, NOW.minus(Duration.ofDays(7)));
        addOrder(7, 2, NOW.minus(Duration.ofDays(7)).minusSeconds(1));
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        ZSetOperations<String, String> zset = mock(ZSetOperations.class);
        when(redis.opsForZSet()).thenReturn(zset);

        new PopularMenuZsetProjection(jdbc, redis, Clock.fixed(NOW, ZoneOffset.UTC)).refresh();

        verify(zset).add(anyString(), eq("1"), eq(2.0));
        verify(redis).rename(anyString(), eq(PopularMenuZsetProjection.KEY));
    }
}
