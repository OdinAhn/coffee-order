package com.example.coffee.point.controller;

import com.example.coffee.point.dto.ChargeRequest;
import com.example.coffee.point.dto.ChargeResponse;
import com.example.coffee.point.service.PointService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/points")
public class PointController {
    private final PointService points;

    public PointController(PointService points) {
        this.points = points;
    }

    @PostMapping("/charges")
    public ResponseEntity<ChargeResponse> charge(@Valid @RequestBody ChargeRequest request) {
        return ResponseEntity.ok(points.charge(request));
    }
}
