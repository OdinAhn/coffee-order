package com.example.coffee;

import lombok.Builder;

public interface OrderEventSender {
    void send(OrderEvent event);

    @Builder
    record OrderEvent(long eventId, long orderId, long userId, long menuId, long paidAmount) {}
}
