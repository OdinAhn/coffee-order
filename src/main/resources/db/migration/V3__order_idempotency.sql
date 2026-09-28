ALTER TABLE orders ADD COLUMN request_key VARCHAR(128);
CREATE UNIQUE INDEX uq_orders_user_request_key ON orders(user_id, request_key);
