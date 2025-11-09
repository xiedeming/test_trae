package com.book.reactive.util;

import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * 用户上下文工具类，用于获取当前登录用户信息
 */
public class UserContextUtil {

    /**
     * 获取当前登录用户ID
     * @param exchange ServerWebExchange对象
     * @return 用户ID的Mono对象
     */
    public static Mono<Long> getCurrentUserId(ServerWebExchange exchange) {
        Long userId = (Long) exchange.getAttributes().get("currentUserId");
        // 如果属性中是Integer类型，需要转换
        if (userId == null) {
            Object obj = exchange.getAttributes().get("currentUserId");
            if (obj instanceof Integer) {
                userId = ((Integer) obj).longValue();
            } else {
                userId = 0L;
            }
        }
        return Mono.just(userId);
    }
    
    /**
     * 获取当前登录用户名称
     * @param exchange ServerWebExchange对象
     * @return 用户名称的Mono对象
     */
    public static Mono<String> getCurrentUserName(ServerWebExchange exchange) {
        String username = (String) exchange.getAttributes().get("currentUsername");
        return Mono.just(username != null ? username : "system");
    }
}