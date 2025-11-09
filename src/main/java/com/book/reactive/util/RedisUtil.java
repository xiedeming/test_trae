package com.book.reactive.util;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import reactor.core.publisher.Mono;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

@Component
public class RedisUtil {

    @Autowired
    private ReactiveRedisTemplate<String, Object> reactiveRedisTemplate;
    
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private static final Logger logger = Logger.getLogger(RedisUtil.class.getName());

    /**
     * 设置缓存 - 将对象序列化为JSON字符串存储
     * @param key 键
     * @param value 值
     * @return Mono<Boolean>
     */
    public Mono<Boolean> set(String key, Object value) {
        try {
            // String jsonValue = objectMapper.writeValueAsString(value);
            return reactiveRedisTemplate.opsForValue().set(key, value)
                    .onErrorResume(error -> {
                        logger.log(Level.WARNING, "Redis set operation failed: " + error.getMessage());
                        // Redis操作失败时返回true以确保流程继续，不影响业务逻辑
                        return Mono.just(true);
                    });
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to serialize object to JSON: " + e.getMessage());
            return Mono.just(true); // 序列化失败也返回true以确保流程继续
        }
    }

    /**
     * 设置缓存并指定过期时间 - 将对象序列化为JSON字符串存储
     * @param key 键
     * @param value 值
     * @param timeout 过期时间
     * @param unit 时间单位
     * @return Mono<Boolean>
     */
    public Mono<Boolean> set(String key, Object value, long timeout, TimeUnit unit) {
        try {
            // String jsonValue = objectMapper.writeValueAsString(value);
            return reactiveRedisTemplate.opsForValue()
                    .set(key, value, java.time.Duration.ofNanos(unit.toNanos(timeout)))
                    .onErrorResume(error -> {
                        logger.log(Level.WARNING, "Redis set operation with expiration failed: " + error.getMessage());
                        // Redis操作失败时返回true以确保流程继续，不影响业务逻辑
                        return Mono.just(true);
                    });
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to serialize object to JSON: " + e.getMessage());
            return Mono.just(true); // 序列化失败也返回true以确保流程继续
        }
    }

    /**
     * 获取缓存
     * @param key 键
     * @param <T> 返回类型
     * @return Mono<T>
     */
    @SuppressWarnings("unchecked")
    public <T> Mono<T> get(String key) {
        return (Mono<T>) reactiveRedisTemplate.opsForValue().get(key)
                .onErrorResume(error -> {
                    logger.log(Level.WARNING, "Redis get operation failed: " + error.getMessage());
                    // Redis操作失败时返回空，允许流程继续到下一个数据源
                    return Mono.empty();
                });
    }
    
    /**
     * 获取缓存并反序列化为指定类型
     * @param key 键
     * @param clazz 目标类型的Class
     * @param <T> 返回类型
     * @return Mono<T>
     */
    public <T> Mono<T> getAndDeserialize(String key, Class<T> clazz) {
        return reactiveRedisTemplate.opsForValue().get(key)
                .flatMap(value -> {
                    try {
                        String valueStr = value.toString();
                        
                        // 检查是否为旧格式（非标准JSON），例如 {id=4, websiteId=1, bookName=全职法师...}
                        if (valueStr.startsWith("{") && valueStr.contains("=")) {
                            logger.info("Detected old format Redis value, converting to standard JSON...");
                            // 尝试将旧格式转换为标准JSON
                            // 1. 将字段名加上双引号
                            String jsonValue = valueStr
                                    .replaceAll("(\\w+)=(?![\\{])", "\"$1\":")
                                    // 2. 将字符串值加上双引号
                                    .replaceAll("=([^,\\{\\]\\s]+)([,}])", "=\"$1\"$2")
                                    // 3. 处理数组和嵌套对象中的=符号（避免错误替换）
                                    .replaceAll("(\\[[^\\]]+)\\s*=\\s*", "$1, ");
                            
                            try {
                                T result = objectMapper.readValue(jsonValue, clazz);
                                return Mono.just(result);
                            } catch (Exception e) {
                                logger.log(Level.WARNING, "Failed to deserialize converted JSON: " + e.getMessage());
                                // 如果转换后仍无法解析，尝试直接返回原始值（如果类型匹配）
                                return handleNonJsonValue(valueStr, clazz);
                            }
                        } else {
                            try {
                                // 尝试标准JSON格式解析
                                T result = objectMapper.readValue(valueStr, clazz);
                                return Mono.just(result);
                            } catch (Exception e) {
                                logger.log(Level.INFO, "Not a standard JSON format, trying to handle as plain text: " + e.getMessage());
                                // 不是有效的JSON格式，尝试作为纯文本处理
                                return handleNonJsonValue(valueStr, clazz);
                            }
                        }
                    } catch (Exception e) {
                        logger.log(Level.WARNING, "Failed to deserialize Redis value: " + e.getMessage());
                        // 反序列化失败返回空，允许流程继续到下一个数据源
                        return Mono.empty();
                    }
                })
                .onErrorResume(error -> {
                    logger.log(Level.WARNING, "Redis get operation failed: " + error.getMessage());
                    // Redis操作失败时返回空，允许流程继续到下一个数据源
                    return Mono.empty();
                });
    }
    
    /**
     * 处理非JSON格式的值
     * @param valueStr 原始字符串值
     * @param clazz 目标类型
     * @param <T> 返回类型
     * @return Mono<T>
     */
    private <T> Mono<T> handleNonJsonValue(String valueStr, Class<T> clazz) {
        try {
            // 如果目标类型是String，直接返回
            if (clazz == String.class) {
                return Mono.just(clazz.cast(valueStr));
            }
            // 如果目标类型是Map，尝试构造一个包含原始值的Map
            else if (clazz == java.util.Map.class || clazz == java.util.HashMap.class) {
                java.util.Map<String, Object> map = new java.util.HashMap<>();
                map.put("content", valueStr);
                map.put("source", "redis-plain-text");
                return Mono.just(clazz.cast(map));
            }
            // 如果目标类型是Book或其他自定义类，这里可能需要特殊处理
            // 但为了简单起见，这里返回空，让流程继续到下一个数据源
            else {
                logger.log(Level.INFO, "Cannot convert plain text to target class: " + clazz.getName());
                return Mono.empty();
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, "Failed to handle non-JSON value: " + e.getMessage());
            return Mono.empty();
        }
    }

    /**
     * 删除缓存
     * @param key 键
     * @return Mono<Boolean>
     */
    public Mono<Boolean> delete(String key) {
        return reactiveRedisTemplate.delete(key)
                .defaultIfEmpty(0L)
                .map(count -> count > 0)
                .onErrorResume(error -> {
                    // Redis操作失败时返回true以确保流程继续，不影响业务逻辑
                    return Mono.just(true);
                });
    }
    
    /**
     * 删除所有匹配给定前缀的键
     * @param prefix 键前缀
     * @return Mono<Boolean>
     */
    public Mono<Boolean> deleteByPrefix(String prefix) {
        return reactiveRedisTemplate.keys(prefix + "*")
                .flatMap(key -> {
                    // 确保每个delete操作都返回Long类型，避免类型转换问题
                    return reactiveRedisTemplate.delete(key)
                            .defaultIfEmpty(0L)
                            .onErrorResume(error -> Mono.just(0L));
                })
                // 使用Long类型进行累加，确保类型安全
                .collectList()
                .map(results -> {
                    // 简单地检查是否有任何删除操作成功，避免可能的类型转换问题
                    return !results.isEmpty() && results.stream().anyMatch(count -> count > 0);
                })
                .onErrorResume(error -> {
                    // 如果整个操作失败，返回true以允许流程继续
                    return Mono.just(true);
                });
    }
    

    /**
     * 检查键是否存在
     * @param key 键
     * @return Mono<Boolean>
     */
    public Mono<Boolean> hasKey(String key) {
        return reactiveRedisTemplate.hasKey(key);
    }

    /**
     * 设置过期时间
     * @param key 键
     * @param timeout 过期时间
     * @param unit 时间单位
     * @return Mono<Boolean>
     */
    public Mono<Boolean> expire(String key, long timeout, TimeUnit unit) {
        return reactiveRedisTemplate.expire(key, java.time.Duration.ofNanos(unit.toNanos(timeout)));
    }
}