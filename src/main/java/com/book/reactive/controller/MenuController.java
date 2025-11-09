package com.book.reactive.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.book.reactive.model.Menu;
import com.book.reactive.service.MenuService;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/menus")
public class MenuController {

    @Autowired
    private MenuService menuService;

    @GetMapping
    public Flux<Menu> getAllMenus() {
        return menuService.findAll();
    }

    @GetMapping("/{id}")
    public Mono<ResponseEntity<Menu>> getMenuById(@PathVariable Long id) {
        return menuService.findById(id)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<Menu> createMenu(@RequestBody Menu menu) {
        return menuService.save(menu);
    }

    @PutMapping("/{id}")
    public Mono<ResponseEntity<Menu>> updateMenu(@PathVariable Long id, @RequestBody Menu menu) {
        return menuService.update(id, menu)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Mono<ResponseEntity<Void>> deleteMenu(@PathVariable Long id) {
        return menuService.delete(id)
                .then(Mono.just(new ResponseEntity<Void>(HttpStatus.NO_CONTENT)));
    }
}