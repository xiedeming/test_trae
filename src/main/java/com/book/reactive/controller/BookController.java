package com.book.reactive.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.book.reactive.model.Book;
import com.book.reactive.scheduler.BookCrawlerJob;
import com.book.reactive.service.BookService;
import com.book.reactive.util.MemoryCacheUtil;
import com.book.reactive.util.RedisUtil;
import com.book.reactive.util.UserContextUtil;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.springframework.web.server.ServerWebExchange;

@RestController
@RequestMapping("/api/books")
public class BookController {

    @Autowired
    private BookService bookService;

    @Autowired
    private RedisUtil redisUtil;

    @Autowired
    private MemoryCacheUtil memoryCacheUtil;
    
    @Autowired
    private BookCrawlerJob bookCrawlerJob;


    // 缓存键前缀
    private static final String BOOK_CACHE_PREFIX = "book:";

    // 获取所有书籍
    @GetMapping
    public Flux<Book> getAllBooks() {
        return bookService.findAll();
    }

    // 根据websiteId获取书籍列表
    @GetMapping("/website/{websiteId}")
    public Flux<Book> getBooksByWebsite(@PathVariable Long websiteId) {
        return bookService.findByWebsiteId(websiteId);
    }

    // 分页查询书籍，支持按书名模糊查询并使用缓存
    @GetMapping("/page")
    public Mono<ResponseEntity<Map<String, Object>>> getBooksWithPagination(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size,
            @RequestParam(required = false) String bookName) {
        // 构建缓存键，包含查询条件和分页参数
        String cacheKey = BOOK_CACHE_PREFIX + "page:" + page + ":size:" + size + ":name:"
                + (bookName != null ? bookName : "");
        String redisKey = BOOK_CACHE_PREFIX + "page:" + page + ":size:" + size + ":name:"
                + (bookName != null ? bookName : "");

        // 1. 先查内存缓存
        Map<String, Object> cachedResult = memoryCacheUtil.get(cacheKey);
        if (cachedResult != null) {
            return Mono.just(ResponseEntity.ok(cachedResult));
        }

        // 2. 再查Redis缓存
        return redisUtil.get(redisKey)
                .flatMap(result -> {
                    // Redis命中，放入内存缓存
                    @SuppressWarnings("unchecked")
                    Map<String, Object> redisResult = (Map<String, Object>) result;
                    memoryCacheUtil.put(cacheKey, redisResult);
                    return Mono.just(ResponseEntity.ok(redisResult));
                })
                // 3. Redis未命中，查数据库
                .switchIfEmpty(bookService.findBooksWithPagination(page, size, bookName)
                        .flatMap(result -> {
                            try {
                                // 数据库查询结果，更新内存缓存
                                memoryCacheUtil.put(cacheKey, result);
                            } catch (Exception e) {
                                // 内存缓存失败不影响主流程
                            }
                            // 分页查询结果设置过期时间（5分钟）
                            return redisUtil.set(redisKey, result, 5, TimeUnit.MINUTES)
                                    .then(Mono.just(ResponseEntity.ok(result)));
                        }));
    }
    
    // 按websiteId分页查询书籍
    @GetMapping("/website/{websiteId}/page")
    public Mono<ResponseEntity<Map<String, Object>>> getBooksByWebsiteWithPagination(
            @PathVariable Long websiteId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size,
            @RequestParam(required = false, defaultValue = "") String bookName) {
        String cacheKey = BOOK_CACHE_PREFIX + "website:" + websiteId + ":page:" + page + ":size:" + size + ":name:" + bookName;
        String redisKey = BOOK_CACHE_PREFIX + "website:" + websiteId + ":page:" + page + ":size:" + size + ":name:" + bookName;
        
        // 先查缓存
        Map<String, Object> cachedResult = memoryCacheUtil.get(cacheKey);
        if (cachedResult != null) {
            return Mono.just(ResponseEntity.ok(cachedResult));
        }
        
        return redisUtil.get(redisKey)
                .flatMap(result -> {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> redisResult = (Map<String, Object>) result;
                    memoryCacheUtil.put(cacheKey, redisResult);
                    return Mono.just(ResponseEntity.ok(redisResult));
                })
                .switchIfEmpty(bookService.findBooksByWebsiteWithPagination(page, size, websiteId, bookName)
                        .flatMap(result -> {
                            try {
                                memoryCacheUtil.put(cacheKey, result);
                            } catch (Exception e) {
                                // 忽略缓存错误
                            }
                            return redisUtil.set(redisKey, result, 5, TimeUnit.MINUTES)
                                    .then(Mono.just(ResponseEntity.ok(result)));
                        }));
    }

    // 根据ID获取书籍 - 实现三级缓存：内存缓存->Redis->MySQL
    @GetMapping("/{id}")
    public Mono<ResponseEntity<Book>> getBookById(@PathVariable Long id) {
        String cacheKey = BOOK_CACHE_PREFIX + id;
        String redisKey = BOOK_CACHE_PREFIX + id;

        // 1. 先查内存缓存
        Book cachedBook = memoryCacheUtil.get(cacheKey);
        if (cachedBook != null) {
            return Mono.just(ResponseEntity.ok(cachedBook));
        }

        // 2. 再查Redis缓存 - 使用getAndDeserialize方法反序列化为Book对象
        return redisUtil.getAndDeserialize(redisKey, Book.class)
                .flatMap(redisBook -> {
                    // Redis命中，放入内存缓存
                    try {
                        memoryCacheUtil.put(cacheKey, redisBook);
                    } catch (Exception e) {
                        // 内存缓存失败不影响主流程
                    }
                    return Mono.just(ResponseEntity.ok(redisBook));
                })
                // 3. Redis未命中，查数据库
                .switchIfEmpty(bookService.findById(id)
                        .flatMap(book -> {
                            // 数据库查询结果，更新缓存
                            try {
                                memoryCacheUtil.put(cacheKey, book);
                            } catch (Exception e) {
                                // 内存缓存失败不影响主流程
                            }
                            // 单个书籍信息可以缓存更长时间（1小时）
                            return redisUtil.set(redisKey, book, 1, TimeUnit.HOURS)
                                    .then(Mono.just(ResponseEntity.ok(book)));
                        })
                        .defaultIfEmpty(ResponseEntity.notFound().build()));
    }

    // 创建新书籍
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<Book> createBook(@RequestBody Book book, ServerWebExchange exchange) {
        LocalDateTime now = LocalDateTime.now();
        return UserContextUtil.getCurrentUserId(exchange)
                .zipWith(UserContextUtil.getCurrentUserName(exchange))
                .flatMap(tuple -> {
                    // 设置创建用户信息，将Integer转换为Long
                    Long userId = tuple.getT1().longValue();
                    book.setCreateUserId(userId);
                    book.setCreateUserName(tuple.getT2());
                    book.setCreateTime(now);
                    // 设置修改用户信息（创建时修改用户与创建用户相同）
                    book.setModifyUserId(userId);
                    book.setModifyUserName(tuple.getT2());
                    book.setModifyTime(now);
                    return bookService.save(book);
                }).flatMap(savedBook -> {
                    // 保存成功后，清除相关分页查询缓存
                    try {
                        memoryCacheUtil.removeAll(BOOK_CACHE_PREFIX + "page:");
                        memoryCacheUtil.removeAll(BOOK_CACHE_PREFIX + "website:");
                    } catch (Exception e) {
                        // 内存缓存清除失败不影响主流程
                    }
                    redisUtil.deleteByPrefix(BOOK_CACHE_PREFIX + "page:").subscribe();
                    redisUtil.deleteByPrefix(BOOK_CACHE_PREFIX + "website:").subscribe();
                    return Mono.just(savedBook);
                });
    }

    // 更新书籍 - 同时更新缓存
    @PutMapping("/{id}")
    public Mono<ResponseEntity<Book>> updateBook(@PathVariable Long id, @RequestBody Book book,
            ServerWebExchange exchange) {
        String cacheKey = BOOK_CACHE_PREFIX + id;
        String redisKey = BOOK_CACHE_PREFIX + id;

        LocalDateTime now = LocalDateTime.now();
        return UserContextUtil.getCurrentUserId(exchange)
                .zipWith(UserContextUtil.getCurrentUserName(exchange))
                .flatMap(tuple -> {
                    // 设置修改用户信息，将Integer转换为Long
                    book.setModifyUserId(tuple.getT1().longValue());
                    book.setModifyUserName(tuple.getT2());
                    book.setModifyTime(now);
                    return bookService.update(id, book);
                })
                .flatMap(updatedBook -> {
                    // 清除相关的分页查询缓存
                    try {
                        memoryCacheUtil.removeAll(BOOK_CACHE_PREFIX + "page:");
                        memoryCacheUtil.removeAll(BOOK_CACHE_PREFIX + "website:");
                    } catch (Exception e) {
                        // 内存缓存清除失败不影响主流程
                    }
                    redisUtil.deleteByPrefix(BOOK_CACHE_PREFIX + "page:").subscribe();
                    redisUtil.deleteByPrefix(BOOK_CACHE_PREFIX + "website:").subscribe();
                    // 更新内存缓存
                    try {
                        memoryCacheUtil.put(cacheKey, updatedBook);
                    } catch (Exception e) {
                        // 内存缓存更新失败不影响主流程
                    }
                    // 更新Redis缓存
                    return redisUtil.set(redisKey, updatedBook, 1, TimeUnit.HOURS)
                            .then(Mono.just(ResponseEntity.ok(updatedBook)));
                })
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    // 删除书籍 - 同时删除缓存
    @DeleteMapping("/{id}")
    public Mono<ResponseEntity<Void>> deleteBook(@PathVariable Long id) {
        String cacheKey = BOOK_CACHE_PREFIX + id;
        String redisKey = BOOK_CACHE_PREFIX + id;
        
        return bookService.delete(id)
                .then(Mono.fromRunnable(() -> {
                    // 清除相关的分页查询缓存
                    try {
                        memoryCacheUtil.removeAll(BOOK_CACHE_PREFIX + "page:");
                        memoryCacheUtil.removeAll(BOOK_CACHE_PREFIX + "website:");
                        memoryCacheUtil.remove(cacheKey);
                    } catch (Exception e) {
                        // 内存缓存清除失败不影响主流程
                    }
                    redisUtil.deleteByPrefix(BOOK_CACHE_PREFIX + "page:").subscribe();
                    redisUtil.deleteByPrefix(BOOK_CACHE_PREFIX + "website:").subscribe();
                    redisUtil.delete(redisKey).subscribe();
                }))
                .then(Mono.just(ResponseEntity.ok().<Void>build()))
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }
    
    // 批量删除书籍
    @DeleteMapping("/batch")
    public Mono<ResponseEntity<Void>> deleteBooks(@RequestBody List<Long> bookIds) {
        return bookService.deleteBatch(Flux.fromIterable(bookIds))
                .then(Mono.fromRunnable(() -> {
                    // 清除相关缓存
                    try {
                        memoryCacheUtil.removeAll(BOOK_CACHE_PREFIX + "page:");
                        memoryCacheUtil.removeAll(BOOK_CACHE_PREFIX + "website:");
                        for (Long id : bookIds) {
                            memoryCacheUtil.remove(BOOK_CACHE_PREFIX + id);
                        }
                    } catch (Exception e) {
                        // 内存缓存清除失败不影响主流程
                    }
                    redisUtil.deleteByPrefix(BOOK_CACHE_PREFIX + "page:").subscribe();
                    redisUtil.deleteByPrefix(BOOK_CACHE_PREFIX + "website:").subscribe();
                    for (Long id : bookIds) {
                        redisUtil.delete(BOOK_CACHE_PREFIX + id).subscribe();
                    }
                }))
                .then(Mono.just(ResponseEntity.ok().<Void>build()));
    }
    
    /**
     * 手动同步书籍
     * @return 同步结果
     */
    @PostMapping("/sync")
    public Mono<ResponseEntity<Map<String, Object>>> syncBooks() {
        return bookCrawlerJob.executeCrawlerJobWithLock()
            .map(lockAcquired -> {
                Map<String, Object> result = new HashMap<>();
                if (lockAcquired) {
                    result.put("success", true);
                    result.put("message", "书籍同步任务已开始执行，请稍后查看结果");
                    return ResponseEntity.ok(result);
                } else {
                    result.put("success", false);
                    result.put("message", "已有同步任务在执行中，请稍后再试");
                    return ResponseEntity.status(HttpStatus.CONFLICT).body(result);
                }
            })
            .onErrorResume(error -> {
                Map<String, Object> result = new HashMap<>();
                result.put("success", false);
                result.put("message", "同步任务启动失败: " + error.getMessage());
                return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result));
            });
    }

}