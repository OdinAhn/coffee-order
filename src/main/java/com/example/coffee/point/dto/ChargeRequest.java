package com.example.coffee.point.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import lombok.Builder;

@Builder
public record ChargeRequest(@Min(1) long userId, @Min(1) @Max(1000000000000L) long amount) {}
