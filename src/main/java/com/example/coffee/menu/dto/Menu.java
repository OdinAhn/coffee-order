package com.example.coffee.menu.dto;

import java.io.Serializable;
import lombok.Builder;

@Builder
public record Menu(long id, String name, long price) implements Serializable {}
