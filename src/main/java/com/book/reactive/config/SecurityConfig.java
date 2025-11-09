package com.book.reactive.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.SecurityWebFiltersOrder;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.http.HttpMethod;
import org.springframework.security.web.server.SecurityWebFilterChain;
import org.springframework.security.web.server.context.NoOpServerSecurityContextRepository;

import com.book.reactive.filter.JwtAuthenticationFilter;

@Configuration
@EnableWebFluxSecurity
public class SecurityConfig {

    @Autowired
    private JwtAuthenticationFilter jwtAuthenticationFilter;

    @Bean
    public SecurityWebFilterChain springSecurityFilterChain(ServerHttpSecurity http) {
        http
            // 禁用CSRF，因为我们使用JWT
            .csrf().disable()
            // 配置安全上下文存储库，使用无状态（不存储会话）
            .securityContextRepository(NoOpServerSecurityContextRepository.getInstance())
            // 添加JWT过滤器
            .addFilterAt(jwtAuthenticationFilter, SecurityWebFiltersOrder.AUTHENTICATION)
            // 配置授权规则
            .authorizeExchange()
            // 允许登录接口匿名访问
            .pathMatchers("/api/users/login").permitAll()
            .pathMatchers("/index.html").permitAll()
            .pathMatchers("/login.html").permitAll()
            .pathMatchers("/books.html").permitAll()
            .pathMatchers("/chapter_detail.html").permitAll()
            .pathMatchers("/book_chapters.html").permitAll()
            .pathMatchers("/favicon.ico").permitAll()
             // 允许静态资源匿名访问
            .pathMatchers("/bootstrap/**").permitAll()
            .pathMatchers("/js/**").permitAll()
            // 其他所有请求都需要认证
            .anyExchange().authenticated();

        return http.build();
    }
}