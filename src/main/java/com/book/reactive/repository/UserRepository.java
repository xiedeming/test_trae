package com.book.reactive.repository;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;

import com.book.reactive.model.User;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface UserRepository extends ReactiveCrudRepository<User, Long> {
    // 根据用户名精确查询，用于登录
    Mono<User> findByUserName(String userName);
    
    // 根据用户名模糊查询用户
    Flux<User> findByUserNameContaining(String userName);
    
    // 根据用户邮箱查询用户
    Mono<User> findByUserEmail(String userEmail);
    
    // 根据用户类型查询用户
    Flux<User> findByUserType(String userType);
    
    // 根据用户角色id查询用户
    Flux<User> findByUserRole(Long userRole);
    
    // 根据手机号查询用户
    Mono<User> findByUserPhone(String userPhone);
}