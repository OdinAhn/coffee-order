package com.example.coffee.menu.dto;

import lombok.Builder;

@Builder
public record PopularMenu(long menuId, String name, long price, long orderCount) {}
