package com.book.reactive.initializer;

import com.book.reactive.model.Book;
import com.book.reactive.model.Chapter;
import com.book.reactive.service.BookService;
import com.book.reactive.service.ChapterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

/**
 * 应用初始化类，用于在应用启动时加载书籍数据到Redis缓存
 */
@Component
public class AppInitializer implements ApplicationRunner {

    private static final Logger logger = LoggerFactory.getLogger(AppInitializer.class);

    @Autowired
    private BookService bookService;

    @Autowired
    private ChapterService chapterService;

    @Autowired
    private ReactiveRedisTemplate<String, Object> reactiveRedisTemplate;

    // 降低并行度以避免数据库连接压力过大
    private  int parallelism = Math.max(5, Math.min(15, Runtime.getRuntime().availableProcessors() * 2));
    private static final String BOOK_CACHE_KEY_PREFIX = "book:";
    private static final String BOOK_NAME_KEY_PREFIX = "book:name:";
    private static final String INIT_BOOK_NAME_KEY = "init:book:name:full:flag";
    private static final String CHAPTER_CACHE_KEY_PREFIX = "chapter:";
    private static final String CHAPTER_URL_KEY_PREFIX = "chapter:url:";
    private static final String CHAPTER_FEATURE_CODE_KEY_PREFIX = "chapter:feature:";
    private static final String INIT_CHAPTER_FEATURE_CODE_KEY = "init:chapter:feature:full:flag";
    @Override
    public void run(ApplicationArguments args) throws Exception {
        logger.info("开始初始化Redis缓存，加载书籍和章节数据...");
        
        CountDownLatch latch = new CountDownLatch(2); // 增加计数，因为现在有两个异步任务
        
        // 检查是否需要执行初始化操作
        boolean needCacheBook = reactiveRedisTemplate.opsForValue().get(INIT_BOOK_NAME_KEY).block() == null;
        boolean needCacheChapter = reactiveRedisTemplate.opsForValue().get(INIT_CHAPTER_FEATURE_CODE_KEY).block() == null;
        
        if (needCacheBook || needCacheChapter) {
            // 只查询一次书籍数据，供两个缓存操作使用
            logger.info("查询数据库中的书籍数据");
            Flux<Book> books = bookService.findAll().cache(); // 使用cache()操作符缓存查询结果
            
            if (needCacheBook) {
                logger.info("书籍名称索引未初始化，开始初始化");
                cacheBook(latch, books);
            } else {
                logger.info("书籍名称索引已初始化，跳过初始化");
                latch.countDown();
            }
            
            if (needCacheChapter) {
                logger.info("章节特征码索引未初始化，开始初始化");
                cacheChapter(latch, books);
            } else {
                logger.info("章节特征码索引已初始化，跳过初始化");
                latch.countDown();
            }
        } else {
            // 都已初始化，直接完成
            logger.info("书籍和章节索引都已初始化，跳过所有初始化操作");
            latch.countDown();
            latch.countDown();
        }
        
        // 等待初始化完成
        latch.await();
        logger.info("Redis缓存初始化全部完成");
    }
    private void cacheChapter(CountDownLatch latch, Flux<Book> books) {
        // 加载所有章节到Redis，使用传入的书籍数据
        books
            .publishOn(Schedulers.boundedElastic())
            .doOnSubscribe(subscription -> logger.info("开始处理章节数据缓存"))
            // 添加错误处理和重试机制
            .flatMap(book -> chapterService.findByBookIds(book.getId())
                .onErrorResume(error -> {
                    logger.warn("查询书籍章节失败，稍后重试: {}，书籍ID: {}, 错误: {}", book.getBookName(), book.getId(), error.getMessage());
                    // 发生错误时返回空流，避免整个流程中断
                    return Flux.empty();
                })
                .delayElements(Duration.ofMillis(50)) // 添加小延迟减少并发压力
            )
            .parallel(parallelism) // 降低并行处理的数量
            .runOn(Schedulers.boundedElastic())
            .flatMap(chapter -> {
                // 存储章节对象
                String chapterKey = CHAPTER_CACHE_KEY_PREFIX + chapter.getId();
                String chapterFeatureCodeKey = CHAPTER_FEATURE_CODE_KEY_PREFIX + chapter.getFeatureCode();
                String chapterUrlKey = CHAPTER_URL_KEY_PREFIX + chapter.getBookId() + ":" + chapter.getChapterUrl();
                
                // 先检查章节特征码索引缓存是否存在（作为主要判断依据）
                return reactiveRedisTemplate.hasKey(chapterFeatureCodeKey)
                    .flatMap(featureCodeExists -> {
                        if (featureCodeExists) {
                            logger.debug("章节缓存已存在(基于特征码)，跳过: {}，书籍ID: {}", chapter.getChapterName(), chapter.getBookId());
                            // 检查并创建章节对象缓存
                            return reactiveRedisTemplate.hasKey(chapterKey)
                                .flatMap(chapterExists -> {
                                    if (chapterExists) {
                                        return Mono.just(false); // 都已存在
                                    }
                                    return reactiveRedisTemplate.opsForValue().set(chapterKey, chapter)
                                        .doOnSuccess(__ -> logger.info("创建章节对象缓存: {}，书籍ID: {}", chapter.getChapterName(), chapter.getBookId()));
                                });
                        } else {
                            logger.info("缓存章节: {}，书籍ID: {}, 特征码: {}", chapter.getChapterName(), chapter.getBookId(), chapter.getFeatureCode());
                            // 章节特征码索引不存在，先设置章节对象缓存
                            return reactiveRedisTemplate.opsForValue().set(chapterKey, chapter)
                                // 然后设置章节特征码索引
                                .then(reactiveRedisTemplate.opsForValue().set(chapterFeatureCodeKey, chapter.getId()))
                                // 同时保留URL索引用于兼容
                                .then(reactiveRedisTemplate.opsForValue().set(chapterUrlKey, chapter.getId()))
                                .then(Mono.just(true));
                        }
                    });
            })
            .sequential() // 恢复为顺序流
            .filter(success -> success) // 只计数新增的缓存
            .count()
            .doOnSuccess(count -> {
                logger.info("章节数据缓存初始化完成，共新增{}章缓存", count);
                reactiveRedisTemplate.opsForValue().set(INIT_CHAPTER_FEATURE_CODE_KEY, 1).subscribe();
                latch.countDown();
            })
            .doOnError(error -> {
                logger.error("章节数据缓存初始化失败: {}", error.getMessage(), error);
                latch.countDown();
            })
            .subscribe();
    }
    private void cacheBook(CountDownLatch latch, Flux<Book> books) {
        // 使用传入的书籍数据
        books
            .publishOn(Schedulers.boundedElastic())
            .doOnSubscribe(subscription -> logger.info("开始处理书籍数据缓存"))
            .delayElements(Duration.ofMillis(30)) // 添加小延迟减少并发压力
            .parallel(parallelism) // 降低并行处理的数量
            .runOn(Schedulers.boundedElastic())
            .flatMap(book -> {
                // 存储书籍对象
                String bookKey = BOOK_CACHE_KEY_PREFIX + book.getId();
                String bookNameKey = BOOK_NAME_KEY_PREFIX + book.getWebsiteId() + ":" + book.getBookName();
                
                // 先检查书籍对象缓存是否存在
                return reactiveRedisTemplate.hasKey(bookKey)
                    .flatMap(bookExists -> {
                        if (bookExists) {
                            logger.debug("书籍缓存已存在，跳过: {}，网站ID: {}", book.getBookName(), book.getWebsiteId());
                            // 检查并创建书籍名称索引
                            return reactiveRedisTemplate.hasKey(bookNameKey)
                                .flatMap(nameIndexExists -> {
                                    if (nameIndexExists) {
                                        return Mono.just(false); // 都已存在
                                    }
                                    return reactiveRedisTemplate.opsForValue().set(bookNameKey, book.getId())
                                        .doOnSuccess(__ -> logger.info("创建书籍名称索引: {}，网站ID: {}", book.getBookName(), book.getWebsiteId()));
                                });
                        } else {
                            logger.info("缓存书籍: {}，网站ID: {}", book.getBookName(), book.getWebsiteId());
                            // 书籍对象缓存不存在，先设置书籍对象缓存
                            return reactiveRedisTemplate.opsForValue().set(bookKey, book)
                                // 然后检查并设置书籍名称索引
                                .then(reactiveRedisTemplate.hasKey(bookNameKey))
                                .flatMap(nameIndexExists -> {
                                    if (nameIndexExists) {
                                        return Mono.just(true);
                                    }
                                    return reactiveRedisTemplate.opsForValue().set(bookNameKey, book.getId())
                                        .then(Mono.just(true));
                                });
                        }
                    });
            })
            .sequential() // 恢复为顺序流
            .filter(success -> success) // 只计数新增的缓存
            .count()
            .doOnSuccess(count -> {
                logger.info("书籍数据缓存初始化完成，共新增{}本书籍缓存", count);
                reactiveRedisTemplate.opsForValue().set(INIT_BOOK_NAME_KEY, 1).subscribe();
                latch.countDown();
            })
            .doOnError(error -> {
                logger.error("书籍数据缓存初始化失败: {}", error.getMessage(), error);
                latch.countDown();
            })
            .subscribe();
    }
}