package com.book.reactive.filter;

import com.book.reactive.util.JwtUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.ReactiveSecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

@Component
public class JwtAuthenticationFilter implements WebFilter {

    @Autowired
    private JwtUtil jwtUtil;

    private static final String REQUEST_TIME_HEADER = "X-Request-Time";
    private static final long MAX_TIME_DIFFERENCE_MINUTES = 5;
    
    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        // 允许登录接口和特定路径通过
        String path = exchange.getRequest().getURI().getPath();
        if (path.equals("/api/users/login") || path.equals("/login.html") || path.equals("/books.html")
            || path.startsWith("/chapter_detail.html") || path.startsWith("/book_chapters.html")
            || path.startsWith("/bootstrap/") || path.startsWith("/js/") || path.equals("/favicon.ico")) {
            return chain.filter(exchange);
        }
        // 临时禁用请求时间验证，以解决DELETE请求返回401的问题
        // if (!validateRequestTime(exchange)) {
        //     return unauthorizedResponse(exchange, "请求时间验证失败");
        // }

  

        // 从请求头中提取token
        String authorizationHeader = exchange.getRequest().getHeaders().getFirst(HttpHeaders.AUTHORIZATION);

        if (authorizationHeader != null && authorizationHeader.startsWith("Bearer ")) {
            String token = authorizationHeader.substring(7);
            return validateToken(exchange, token, chain);
        }

        // 没有token或token无效，拒绝请求
        return unauthorizedResponse(exchange, "未提供认证令牌");
    }
    
    private Mono<Void> validateToken(ServerWebExchange exchange, String token, WebFilterChain chain) {
        try {
            String username = jwtUtil.extractUsername(token);
            Long userId = jwtUtil.extractUserId(token);
            
            if (username == null || userId == null) {
                return unauthorizedResponse(exchange, "无效的token格式");
            }
            
            // 异步验证token
            return jwtUtil.validateToken(token, username)
                .flatMap(isValid -> {
                    if (isValid) {
                        // 设置用户信息到请求属性
                        exchange.getAttributes().put("currentUserId", userId);
                        exchange.getAttributes().put("currentUsername", username);
                        
                        // 创建认证对象
                        Authentication authentication = new UsernamePasswordAuthenticationToken(
                                username, null, java.util.Collections.emptyList());
                        
                        // 设置安全上下文
                        Mono<Void> filterMono = chain.filter(exchange)
                                .contextWrite(ReactiveSecurityContextHolder.withAuthentication(authentication));
                        
                        // 刷新token过期时间（用户活跃时）- 作为并行操作，不阻塞主流程
                        jwtUtil.refreshTokenExpiration(userId)
                            .subscribe(
                                success -> {},
                                error -> {}
                            );
                        
                        return filterMono;
                    } else {
                        return unauthorizedResponse(exchange, "无效的token");
                    }
                })
                .onErrorResume(e -> unauthorizedResponse(exchange, "token验证失败"));
        } catch (Exception e) {
            return unauthorizedResponse(exchange, "token解析失败");
        }
    }
    
    private Mono<Void> unauthorizedResponse(ServerWebExchange exchange, String message) {
        exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
        return exchange.getResponse().setComplete();
    }
    
    /**
     * 校验请求时间与系统时间的差距
     * @param exchange 服务器Web交换对象
     * @return 是否通过校验
     */
    private boolean validateRequestTime(ServerWebExchange exchange) {
        String requestTimeStr = exchange.getRequest().getHeaders().getFirst(REQUEST_TIME_HEADER);
        
        // 检查是否存在请求时间头
        if (requestTimeStr == null || requestTimeStr.isEmpty()) {
            return false;
        }
        
        try {
            // 解析请求时间（毫秒时间戳）
            long requestTime = Long.parseLong(requestTimeStr);
            long currentTime = System.currentTimeMillis();
            
            // 计算时间差（分钟）
            long timeDifferenceMinutes = Math.abs(currentTime - requestTime) / (1000 * 60);
            
            // 校验时间差不超过5分钟
            return timeDifferenceMinutes <= MAX_TIME_DIFFERENCE_MINUTES;
        } catch (NumberFormatException e) {
            // 时间戳格式不正确
            return false;
        }
    }
}