package com.example.coffee.point.dto;

import lombok.Builder;

@Builder
public record ChargeResponse(long userId, long balance) {}
