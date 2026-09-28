CREATE TABLE collected_order_events (
    event_id BIGINT PRIMARY KEY,
    order_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    menu_id BIGINT NOT NULL,
    paid_amount BIGINT NOT NULL,
    collected_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
);
