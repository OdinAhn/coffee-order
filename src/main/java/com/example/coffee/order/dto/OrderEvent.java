package com.example.coffee.order.dto;

import lombok.Builder;

@Builder
public record OrderEvent(long eventId, long orderId, long userId, long menuId, long paidAmount) {}
