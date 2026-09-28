package com.example.coffee.order.dto;

import java.time.Instant;
import lombok.Builder;

@Builder
public record OrderResponse(long orderId, long userId, long menuId, long paidAmount, Instant orderedAt) {}
