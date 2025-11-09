package com.book.reactive.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import org.springframework.web.reactive.config.ResourceHandlerRegistry;
import org.springframework.web.reactive.config.WebFluxConfigurer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Web配置类，用于配置静态资源访问规则和全局Jackson配置
 */
@Configuration
public class WebConfig implements WebFluxConfigurer {
    
    /**
     * 配置全局的Jackson ObjectMapper，确保日期时间以字符串格式返回
     */
    @Bean
    public ObjectMapper objectMapper() {
        // 配置ObjectMapper
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
        
        return objectMapper;
    }
    
    /**
     * 配置Jackson2ObjectMapperBuilder，确保全局使用相同的日期格式设置
     */
    @Bean
    public Jackson2ObjectMapperBuilder jackson2ObjectMapperBuilder() {
        // 创建Jackson2ObjectMapperBuilder并配置日期时间序列化格式
        return new Jackson2ObjectMapperBuilder()
                .serializerByType(LocalDateTime.class, 
                    new LocalDateTimeSerializer(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))
                .featuresToDisable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .serializationInclusion(JsonInclude.Include.NON_NULL);
    }
    
    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        // 配置静态资源访问规则，允许访问bootstrap和js文件夹
        registry.addResourceHandler("/bootstrap/**")
                .addResourceLocations("classpath:/static/bootstrap/")
                .resourceChain(false);
        
        registry.addResourceHandler("/js/**")
                .addResourceLocations("classpath:/static/js/")
                .resourceChain(false);
        
        // 确保默认的静态资源处理仍能正常工作
        registry.addResourceHandler("/**")
                .addResourceLocations("classpath:/static/")
                .resourceChain(false);
    }
}