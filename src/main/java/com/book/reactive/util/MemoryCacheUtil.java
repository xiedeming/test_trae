package com.book.reactive.util;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

@Component
public class MemoryCacheUtil {

    // 缓存项，包含数据和过期时间
    private static class CacheItem<T> {
        private final T data;
        private long expireTime;
        
        public CacheItem(T data, long ttl) {
            this.data = data;
            this.expireTime = System.currentTimeMillis() + ttl;
        }
        
        public boolean isExpired() {
            return System.currentTimeMillis() > expireTime;
        }
        
        public void refresh(long ttl) {
            this.expireTime = System.currentTimeMillis() + ttl;
        }
        
        public T getData() {
            return data;
        }
    }
    
    // 默认缓存过期时间（毫秒）
    private static final long DEFAULT_CACHE_TTL = 600000; // 10分钟
    
    // 缓存容器，使用ConcurrentHashMap保证线程安全
    private final Map<String, CacheItem<?>> cache = new ConcurrentHashMap<>();
    
    /**
     * 获取缓存
     * @param key 缓存键
     * @param <T> 返回类型
     * @return 缓存的数据，如果不存在或已过期则返回null
     */
    @SuppressWarnings("unchecked")
    public <T> T get(String key) {
        CacheItem<T> item = (CacheItem<T>) cache.get(key);
        if (item != null) {
            if (item.isExpired()) {
                // 缓存已过期，移除
                cache.remove(key);
                return null;
            } else {
                // 缓存命中，刷新过期时间（热点缓存特性）
                item.refresh(DEFAULT_CACHE_TTL);
                return item.getData();
            }
        }
        return null;
    }
    
    /**
     * 获取缓存，如果不存在则通过supplier加载
     * @param key 缓存键
     * @param supplier 数据加载函数
     * @param <T> 返回类型
     * @return 缓存的数据
     */
    public <T> T get(String key, Function<String, T> supplier) {
        return get(key, supplier, DEFAULT_CACHE_TTL);
    }
    
    /**
     * 获取缓存，如果不存在则通过supplier加载，并指定过期时间
     * @param key 缓存键
     * @param supplier 数据加载函数
     * @param ttl 过期时间（毫秒）
     * @param <T> 返回类型
     * @return 缓存的数据
     */
    public <T> T get(String key, Function<String, T> supplier, long ttl) {
        T value = get(key);
        if (value == null && supplier != null) {
            value = supplier.apply(key);
            if (value != null) {
                put(key, value, ttl);
            }
        }
        return value;
    }
    
    /**
     * 设置缓存
     * @param key 缓存键
     * @param value 缓存值
     */
    public <T> void put(String key, T value) {
        put(key, value, DEFAULT_CACHE_TTL);
    }
    
    /**
     * 设置缓存并指定过期时间
     * @param key 缓存键
     * @param value 缓存值
     * @param ttl 过期时间（毫秒）
     */
    public <T> void put(String key, T value, long ttl) {
        cache.put(key, new CacheItem<>(value, ttl));
    }
    
    /**
     * 删除缓存
     * @param key 缓存键
     */
    public void remove(String key) {
        cache.remove(key);
    }

    /**
     * 删除所有以指定前缀开头的缓存
     * @param prefix 缓存键前缀
     */
    public void removeAll(String prefix) {
        cache.keySet().removeIf(k -> k.startsWith(prefix));
    }
    
    /**
     * 清除所有缓存
     */
    public void clear() {
        cache.clear();
    }
    
    /**
     * 检查缓存是否存在
     * @param key 缓存键
     * @return 是否存在且未过期
     */
    public boolean containsKey(String key) {
        return get(key) != null;
    }
    
    /**
     * 获取缓存大小
     * @return 缓存项数量
     */
    public int size() {
        // 清理过期项并返回有效缓存数量
        cache.forEach((key, item) -> {
            if (item.isExpired()) {
                cache.remove(key);
            }
        });
        return cache.size();
    }
}