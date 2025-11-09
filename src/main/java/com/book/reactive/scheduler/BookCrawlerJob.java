package com.book.reactive.scheduler;

import com.book.reactive.model.Website;
import com.book.reactive.service.BookCrawlerService;
import com.book.reactive.service.WebsiteService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 书籍爬虫定时任务
 */
@Component
public class BookCrawlerJob {

    private static final Logger logger = LoggerFactory.getLogger(BookCrawlerJob.class);

    @Autowired
    private WebsiteService websiteService;

    @Autowired
    private BookCrawlerService bookCrawlerService;

    @Value("${book.crawl.cron}")
    private String bookCrawlCron;

    /**
     * 定时执行书籍爬虫任务
     */
    @Scheduled(cron = "${book.crawl.cron}")
    public void executeCrawlerJob() {
        logger.info("========================================");
        logger.info("[定时任务] 开始执行书籍爬虫定时任务 - 当前时间: {}", LocalDateTime.now());

        try {
            logger.info("[定时任务] 尝试获取所有网站信息");
            // 获取所有网站信息，添加超时处理
            List<Website> websites = websiteService.findAll()
                    .collectList()
                    .timeout(java.time.Duration.ofMinutes(1))
                    .block();
            
            if (websites == null || websites.isEmpty()) {
                logger.warn("[定时任务] 没有找到网站信息，爬虫任务终止");
                return;
            }

            logger.info("[定时任务] 发现{}个网站需要爬取: {}", websites.size(), 
                        websites.stream().map(Website::getWebsiteName).collect(java.util.stream.Collectors.joining(", ")));

            // 使用反应式方式处理每个网站的爬取，避免使用CountDownLatch
            Flux.fromIterable(websites)
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
                .subscribe(); // 确保反应式流被订阅执行

        } catch (Exception e) {
            logger.error("[定时任务] 书籍爬虫定时任务执行失败: {}", e.getMessage(), e);
            logger.info("[定时任务] 书籍爬虫定时任务执行结束（异常） - 结束时间: {}", LocalDateTime.now());
        }
    }
}