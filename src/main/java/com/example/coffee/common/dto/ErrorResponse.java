package com.example.coffee.common.dto;

import lombok.Builder;

@Builder
public record ErrorResponse(String code, String message) {}
