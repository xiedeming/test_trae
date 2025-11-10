package com.book.reactive.scheduler;

import com.book.reactive.model.Website;
import com.book.reactive.service.BookCrawlerService;
import com.book.reactive.service.WebsiteService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 书籍爬虫定时任务
 */
@Component
public class BookCrawlerJob {

    private static final Logger logger = LoggerFactory.getLogger(BookCrawlerJob.class);
    private static final String CRAWLER_JOB_LOCK_KEY = "book:crawler:job:lock";
    private static final long LOCK_EXPIRE_TIME = 6 * 60 * 60 * 1000; // 锁过期时间6小时

    @Autowired
    private WebsiteService websiteService;

    @Autowired
    private BookCrawlerService bookCrawlerService;
    
    @Autowired
    private ReactiveRedisTemplate<String, String> reactiveRedisTemplate;

    @Value("${book.crawl.cron}")
    private String bookCrawlCron;

    /**
     * 执行书籍爬虫任务（带分布式锁）
     * @return 是否成功获取锁并执行任务
     */
    public Mono<Boolean> executeCrawlerJobWithLock() {
        String lockValue = UUID.randomUUID().toString();
        
        // 尝试获取分布式锁
        return reactiveRedisTemplate.opsForValue().setIfAbsent(CRAWLER_JOB_LOCK_KEY, lockValue, 
                Duration.ofMillis(LOCK_EXPIRE_TIME))
            .flatMap(lockAcquired -> {
                if (lockAcquired) {
                    logger.info("[手动触发] 成功获取爬虫任务锁，开始执行爬虫任务 - 当前时间: {}", LocalDateTime.now());
                    return doExecuteCrawlerJob()
                        .doFinally(signalType -> releaseLock(lockValue))
                        .thenReturn(true);
                } else {
                    logger.warn("[手动触发] 爬虫任务已在执行中，无法获取锁");
                    return Mono.just(false);
                }
            });
    }
    
    /**
     * 定时执行书籍爬虫任务
     */
    @Scheduled(cron = "${book.crawl.cron}")
    public void executeCrawlerJob() {
        logger.info("========================================");
        logger.info("[定时任务] 开始执行书籍爬虫定时任务 - 当前时间: {}", LocalDateTime.now());
        
        String lockValue = UUID.randomUUID().toString();
        
        // 尝试获取分布式锁
        reactiveRedisTemplate.opsForValue().setIfAbsent(CRAWLER_JOB_LOCK_KEY, lockValue, 
                Duration.ofMillis(LOCK_EXPIRE_TIME))
            .subscribe(lockAcquired -> {
                if (lockAcquired) {
                    try {
                        doExecuteCrawlerJob().subscribe();
                    } catch (Exception e) {
                        logger.error("[定时任务] 书籍爬虫定时任务执行失败: {}", e.getMessage(), e);
                        logger.info("[定时任务] 书籍爬虫定时任务执行结束（异常） - 结束时间: {}", LocalDateTime.now());
                    } finally {
                        releaseLock(lockValue);
                    }
                } else {
                    logger.warn("[定时任务] 爬虫任务已在执行中，跳过本次定时任务");
                }
            });
        
        return;
        
    }
    
    /**
     * 执行爬虫任务的核心逻辑
     */
    private Mono<Void> doExecuteCrawlerJob() {
        logger.info("[爬虫任务] 开始执行实际的爬虫逻辑");

        try {
            logger.info("[定时任务] 尝试获取所有网站信息");
            // 获取所有网站信息，添加超时处理
            List<Website> websites = websiteService.findAll()
                    .collectList()
                    .timeout(java.time.Duration.ofMinutes(1))
                    .block();
            
            if (websites == null || websites.isEmpty()) {
                logger.warn("[定时任务] 没有找到网站信息，爬虫任务终止");
                return Mono.empty();
            }

            logger.info("[定时任务] 发现{}个网站需要爬取: {}", websites.size(), 
                        websites.stream().map(Website::getWebsiteName).collect(java.util.stream.Collectors.joining(", ")));

            // 使用反应式方式处理每个网站的爬取，避免使用CountDownLatch
            return Flux.fromIterable(websites)
                .publishOn(Schedulers.boundedElastic())
                .flatMap(website -> {
                    logger.info("[定时任务] 开始爬取网站: {}, URL: {}", 
                            website.getWebsiteName(), website.getWebsiteUrl());
                    return bookCrawlerService.crawlWebsiteBooks(website)
                        .publishOn(Schedulers.boundedElastic()) // 确保在正确的线程上执行
                        .timeout(java.time.Duration.ofMinutes(30)) // 设置每个网站爬取的超时时间
                        .doOnSuccess(result -> {
                            logger.info("[定时任务] 网站爬取完成: {}, 结果: {}", 
                                    website.getWebsiteName(), result ? "成功" : "失败");
                        })
                        .doOnError(error -> {
                            logger.error("[定时任务] 爬取网站失败: {} - {}", 
                                    website.getWebsiteName(), error.getMessage(), error);
                        })
                        .onErrorReturn(false);
                }, 2) // 限制并发爬取的网站数量为2，避免系统负载过高
                .collectList()
                .doOnSuccess(results -> {
                    long successCount = results.stream().filter(result -> result).count();
                    logger.info("[定时任务] 所有网站爬取任务完成 - 成功数: {}, 总数: {}",
                            successCount, results.size());
                    logger.info("[定时任务] 书籍爬虫定时任务执行完成 - 结束时间: {}", LocalDateTime.now());
                })
                .doOnError(error -> {
                    logger.error("[定时任务] 批量爬取过程中发生异常: {}", error.getMessage(), error);
                })
                .then(); // 转换为Mono<Void>

        } catch (Exception e) {
            logger.error("[爬虫任务] 书籍爬虫任务执行失败: {}", e.getMessage(), e);
            logger.info("[爬虫任务] 书籍爬虫任务执行结束（异常） - 结束时间: {}", LocalDateTime.now());
            return Mono.empty();
        }
    }
    
    /**
     * 释放分布式锁（使用Lua脚本确保原子性）
     */
    private void releaseLock(String lockValue) {
        String script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
        RedisScript<Long> redisScript = RedisScript.of(script, Long.class);
        
        reactiveRedisTemplate.execute(redisScript, Collections.singletonList(CRAWLER_JOB_LOCK_KEY), Collections.singletonList(lockValue))
            .subscribe(result -> {
                if (result > 0) {
                    logger.info("[爬虫任务] 成功释放爬虫任务锁");
                } else {
                    logger.warn("[爬虫任务] 无法释放爬虫任务锁，可能已过期");
                }
            }, error -> {
                logger.error("[爬虫任务] 释放锁时发生错误: {}", error.getMessage(), error);
            });
    }
}