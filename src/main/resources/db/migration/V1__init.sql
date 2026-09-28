CREATE TABLE menus (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    price BIGINT NOT NULL CHECK (price > 0)
);

CREATE TABLE point_accounts (
    user_id BIGINT PRIMARY KEY,
    balance BIGINT NOT NULL DEFAULT 0 CHECK (balance >= 0 AND balance <= 1000000000000)
);

CREATE TABLE orders (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES point_accounts(user_id),
    menu_id BIGINT NOT NULL REFERENCES menus(id),
    paid_amount BIGINT NOT NULL CHECK (paid_amount > 0),
    ordered_at TIMESTAMP(6) NOT NULL
);

CREATE INDEX idx_orders_recent ON orders(ordered_at, menu_id);

CREATE TABLE order_outbox (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    order_id BIGINT NOT NULL UNIQUE REFERENCES orders(id),
    user_id BIGINT NOT NULL,
    menu_id BIGINT NOT NULL,
    paid_amount BIGINT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMP(6) NOT NULL
);

CREATE INDEX idx_order_outbox_ready ON order_outbox(next_attempt_at, id);

INSERT INTO menus(name, price) VALUES
    ('Americano', 4500),
    ('Cafe Latte', 5000),
    ('Cappuccino', 5500),
    ('Cold Brew', 6000);
