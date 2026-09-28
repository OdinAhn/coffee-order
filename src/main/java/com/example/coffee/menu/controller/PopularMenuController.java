package com.example.coffee.menu.controller;

import com.example.coffee.menu.dto.PopularMenu;
import com.example.coffee.menu.service.PopularMenuService;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/menus/popular")
public class PopularMenuController {
    private final PopularMenuService popularMenus;

    public PopularMenuController(PopularMenuService popularMenus) {
        this.popularMenus = popularMenus;
    }

    @GetMapping
    public ResponseEntity<List<PopularMenu>> list() {
        return ResponseEntity.ok(popularMenus.list());
    }
}
