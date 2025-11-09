package com.book.reactive.util;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import reactor.core.publisher.Mono;

@Component
public class JwtUtil {

    @Value("${jwt.secret}")
    private String secretKey;

    // 设置token过期时间为30分钟
    private static final long TOKEN_EXPIRATION = 30 * 60 * 1000;
    
    // Redis key前缀
    private static final String TOKEN_PREFIX = "user_token:";

    @Autowired
    private RedisUtil redisUtil;

    // 从token中提取用户名
    public String extractUsername(String token) {
        return extractClaim(token, Claims::getSubject);
    }

    // 从token中提取过期时间
    public Date extractExpiration(String token) {
        return extractClaim(token, Claims::getExpiration);
    }

    // 提取token中的声明
    public <T> T extractClaim(String token, Function<Claims, T> claimsResolver) {
        final Claims claims = extractAllClaims(token);
        return claimsResolver.apply(claims);
    }

    // 从token中提取所有声明
    private Claims extractAllClaims(String token) {
        return Jwts.parser().setSigningKey(secretKey).parseClaimsJws(token).getBody();
    }

    // 检查token是否过期
    private Boolean isTokenExpired(String token) {
        return extractExpiration(token).before(new Date());
    }

    // 生成token
    public Mono<String> generateToken(Long userId, String username) {
        // 清除该用户之前的token（实现单用户单token）
        String userTokenKey = TOKEN_PREFIX + userId;
        
        return redisUtil.delete(userTokenKey)
            .then(Mono.fromCallable(() -> {
                Map<String, Object> claims = new HashMap<>();
                claims.put("userId", userId);
                return createToken(claims, username);
            }))
            .flatMap(token -> 
                // 将新token存储到Redis，设置过期时间为30分钟
                redisUtil.set(userTokenKey, token, 30, TimeUnit.MINUTES)
                    .then(Mono.just(token))
            );
    }

    // 创建token
    private String createToken(Map<String, Object> claims, String subject) {
        return Jwts.builder()
                .setClaims(claims)
                .setSubject(subject)
                .setIssuedAt(new Date(System.currentTimeMillis()))
                .setExpiration(new Date(System.currentTimeMillis() + TOKEN_EXPIRATION))
                .signWith(SignatureAlgorithm.HS256, secretKey)
                .compact();
    }

    // 验证token
    public Mono<Boolean> validateToken(String token, String username) {
        try {
            final String extractedUsername = extractUsername(token);
            final Long userId = extractUserId(token);
            
            // 检查token是否过期
            if (isTokenExpired(token)) {
                return Mono.just(false);
            }
            
            // 检查token是否与Redis中存储的一致（验证是否是当前有效的token）
            String userTokenKey = TOKEN_PREFIX + userId;
            return redisUtil.get(userTokenKey)
                .map(storedToken -> storedToken != null && storedToken.equals(token) && extractedUsername.equals(username))
                .defaultIfEmpty(false);
        } catch (Exception e) {
            return Mono.just(false);
        }
    }
    
    // 刷新token的过期时间（用于用户活跃时）
    public Mono<Boolean> refreshTokenExpiration(Long userId) {
        String userTokenKey = TOKEN_PREFIX + userId;
        return redisUtil.hasKey(userTokenKey)
            .flatMap(exists -> {
                if (exists) {
                    // 重置过期时间为30分钟
                    return redisUtil.expire(userTokenKey, 30, TimeUnit.MINUTES);
                }
                return Mono.just(false);
            });
    }
    
    // 手动登出用户（删除token）
    public Mono<Boolean> logoutUser(Long userId) {
        String userTokenKey = TOKEN_PREFIX + userId;
        return redisUtil.delete(userTokenKey);
    }

    // 从token中提取用户ID
    public Long extractUserId(String token) {
        final Claims claims = extractAllClaims(token);
        return Long.valueOf(claims.get("userId").toString());
    }
}