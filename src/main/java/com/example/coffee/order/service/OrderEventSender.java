package com.example.coffee.order.service;

import com.example.coffee.order.dto.OrderEvent;

public interface OrderEventSender {
    void send(OrderEvent event);
}
