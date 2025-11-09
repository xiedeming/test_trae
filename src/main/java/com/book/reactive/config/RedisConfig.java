package com.book.reactive.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Configuration
public class RedisConfig {

    @Bean
    public ReactiveRedisTemplate<String, Object> reactiveRedisTemplate(ReactiveRedisConnectionFactory factory) {
        // 使用StringRedisSerializer作为键的序列化器
        RedisSerializer<String> keySerializer = new StringRedisSerializer();
        
        // 配置ObjectMapper以支持Java 8日期时间类型并设置格式化
        ObjectMapper objectMapper = new ObjectMapper();
        
        // 添加JavaTimeModule并配置LocalDateTime序列化格式
        JavaTimeModule timeModule = new JavaTimeModule();
        // 配置LocalDateTime的序列化格式为yyyy-MM-dd HH:mm:ss
        timeModule.addSerializer(LocalDateTime.class, 
            new LocalDateTimeSerializer(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        objectMapper.registerModule(timeModule);
        
        // 禁用日期作为时间戳
        objectMapper.disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        
        // 设置非空字段才序列化
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        
        objectMapper.findAndRegisterModules();
        
        // 使用Jackson2JsonRedisSerializer作为值的序列化器，并配置ObjectMapper
        Jackson2JsonRedisSerializer<Object> valueSerializer = new Jackson2JsonRedisSerializer<>(Object.class);
        valueSerializer.setObjectMapper(objectMapper);
        
        // 配置序列化上下文
        RedisSerializationContext.RedisSerializationContextBuilder<String, Object> builder = 
                RedisSerializationContext.newSerializationContext(keySerializer);
        RedisSerializationContext<String, Object> context = 
                builder.value(valueSerializer).hashValue(valueSerializer).build();
        
        // 创建并返回ReactiveRedisTemplate
        return new ReactiveRedisTemplate<>(factory, context);
    }
}