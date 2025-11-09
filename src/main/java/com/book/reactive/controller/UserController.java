package com.book.reactive.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.book.reactive.model.User;
import com.book.reactive.model.dto.LoginRequest;
import com.book.reactive.model.dto.LoginResponse;
import com.book.reactive.service.UserService;
import com.book.reactive.util.UserContextUtil;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.web.server.ServerWebExchange;

@RestController
@RequestMapping("/api/users")
public class UserController {

    @Autowired
    private UserService userService;
    
    // 登录接口
    @PostMapping("/login")
    public Mono<ResponseEntity<LoginResponse>> login(@RequestBody LoginRequest loginRequest) {
        return userService.login(loginRequest)
                .map(ResponseEntity::ok)
                .onErrorResume(e -> Mono.just(ResponseEntity.status(HttpStatus.UNAUTHORIZED).build()));
    }

    // 获取所有用户
    @GetMapping
    public Flux<User> getAllUsers(ServerWebExchange exchange) {
        return userService.findAll();
    }

    // 根据ID获取用户
    @GetMapping("/{id}")
    public Mono<ResponseEntity<User>> getUserById(@PathVariable Long id, ServerWebExchange exchange) {
        return userService.findById(id)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    // 创建新用户
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<User> createUser(@RequestBody User user, ServerWebExchange exchange) {
        // 使用当前登录用户信息作为创建者
        return UserContextUtil.getCurrentUserId(exchange)
                .zipWith(UserContextUtil.getCurrentUserName(exchange))
                .flatMap(tuple -> userService.save(user, tuple.getT1(), tuple.getT2()));
    }

    // 更新用户
    @PutMapping("/{id}")
    public Mono<ResponseEntity<User>> updateUser(@PathVariable Long id, @RequestBody User user, ServerWebExchange exchange) {
        // 使用当前登录用户信息作为修改者
        return UserContextUtil.getCurrentUserId(exchange)
                .zipWith(UserContextUtil.getCurrentUserName(exchange))
                .flatMap(tuple -> userService.update(id, user, tuple.getT1(), tuple.getT2()))
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    // 删除用户
    @DeleteMapping("/{id}")
    public Mono<ResponseEntity<Void>> deleteUser(@PathVariable Long id, ServerWebExchange exchange) {
        return userService.delete(id)
                .then(Mono.just(new ResponseEntity<Void>(HttpStatus.NO_CONTENT)))
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    // 根据用户名模糊查询用户
    @GetMapping("/name/search")
    public Flux<User> getUsersByNameContaining(@RequestParam String name, ServerWebExchange exchange) {
        return userService.findByUserNameContaining(name);
    }
    
    // 根据邮箱查询用户
    @GetMapping("/email/{email}")
    public Mono<ResponseEntity<User>> getUserByEmail(@PathVariable String email, ServerWebExchange exchange) {
        return userService.findByUserEmail(email)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }
    
    // 根据用户类型查询用户
    @GetMapping("/type/{type}")
    public Flux<User> getUsersByType(@PathVariable String type, ServerWebExchange exchange) {
        return userService.findByUserType(type);
    }
    
    // 根据角色ID查询用户
    @GetMapping("/role/{roleId}")
    public Flux<User> getUsersByRole(@PathVariable Long roleId, ServerWebExchange exchange) {
        return userService.findByUserRole(roleId);
    }
    
    // 根据手机号查询用户
    @GetMapping("/phone/{phone}")
    public Mono<ResponseEntity<User>> getUserByPhone(@PathVariable String phone, ServerWebExchange exchange) {
        return userService.findByUserPhone(phone)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }
}