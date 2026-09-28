package com.example.coffee.order.dto;

import jakarta.validation.constraints.Min;
import lombok.Builder;

@Builder
public record OrderRequest(@Min(1) long userId, @Min(1) long menuId) {}
