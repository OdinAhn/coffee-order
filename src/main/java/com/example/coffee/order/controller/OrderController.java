package com.example.coffee.order.controller;

import com.example.coffee.order.dto.OrderRequest;
import com.example.coffee.order.dto.OrderResponse;
import com.example.coffee.order.service.OrderService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/orders")
public class OrderController {
    private final OrderService orders;

    public OrderController(OrderService orders) {
        this.orders = orders;
    }

    @PostMapping
    public ResponseEntity<OrderResponse> place(@Valid @RequestBody OrderRequest request,
                                               @RequestHeader("Idempotency-Key") String requestKey) {
        return ResponseEntity.status(HttpStatus.CREATED).body(orders.place(request, requestKey));
    }
}
