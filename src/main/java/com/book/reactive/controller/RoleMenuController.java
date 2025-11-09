package com.book.reactive.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.book.reactive.model.RoleMenu;
import com.book.reactive.service.RoleMenuService;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/role-menus")
public class RoleMenuController {

    @Autowired
    private RoleMenuService roleMenuService;

    @GetMapping
    public Flux<RoleMenu> getAllRoleMenus() {
        return roleMenuService.findAll();
    }

    @GetMapping("/{id}")
    public Mono<ResponseEntity<RoleMenu>> getRoleMenuById(@PathVariable Long id) {
        return roleMenuService.findById(id)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<RoleMenu> createRoleMenu(@RequestBody RoleMenu roleMenu) {
        return roleMenuService.save(roleMenu);
    }

    @PutMapping("/{id}")
    public Mono<ResponseEntity<RoleMenu>> updateRoleMenu(@PathVariable Long id, @RequestBody RoleMenu roleMenu) {
        return roleMenuService.update(id, roleMenu)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Mono<ResponseEntity<Void>> deleteRoleMenu(@PathVariable Long id) {
        return roleMenuService.delete(id)
                .then(Mono.just(new ResponseEntity<Void>(HttpStatus.NO_CONTENT)));
    }
}