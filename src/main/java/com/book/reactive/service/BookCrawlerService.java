package com.book.reactive.service;

import com.book.reactive.model.Book;
import com.book.reactive.model.ChapterTask;
import com.book.reactive.model.Website;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * 书籍爬虫服务类
 */
@Service
public class BookCrawlerService {

    private static final Logger logger = LoggerFactory.getLogger(BookCrawlerService.class);

    @Autowired
    private BookService bookService;

    @Autowired
    private RabbitMQService rabbitMQService;

    @Autowired
    private ReactiveRedisTemplate<String, Object> reactiveRedisTemplate;

    private static final String BOOK_NAME_KEY_PREFIX = "book:name:";

    private final WebClient webClient;

    public BookCrawlerService() {
        this.webClient = WebClient.builder()
                .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(12 * 1024 * 1024)) // 12MB
                .build();
    }

    /**
     * 爬取指定网站的书籍信息
     */
    public Mono<Boolean> crawlWebsiteBooks(Website website) {
        return fetchHtml(website.getWebsiteUrl())
                .flatMap(html -> parseBooks(html, website))
                .doOnSuccess(result -> logger.info("网站爬取完成: {}", website.getWebsiteName()))
                .doOnError(error -> logger.error("网站爬取失败: {} - {}", website.getWebsiteName(), error.getMessage(), error));
    }

    /**
     * 获取网页HTML内容
     */
    private Mono<String> fetchHtml(String url) {
        logger.info("正在访问: {}", url);
        return webClient.get()
                .uri(url)
                .retrieve()
                .bodyToMono(String.class)
                .subscribeOn(Schedulers.boundedElastic())
                .timeout(java.time.Duration.ofSeconds(30))
                .onErrorResume(e -> {
                    logger.error("访问失败: {} - {}", url, e.getMessage());
                    return Mono.empty();
                });
    }

    /**
     * 解析书籍信息
     */
    private Mono<Boolean> parseBooks(String html, Website website) {
        // 检查HTML是否为空
        if (html == null || html.isEmpty()) {
            logger.warn("HTML内容为空，跳过解析: {}", website.getWebsiteName());
            return Mono.just(false);
        }

        try {
            Document doc = Jsoup.parse(html);
            Elements bookElements = doc.select(".shop");
            logger.info("找到书籍数量: {}，网站: {}", bookElements.size(), website.getWebsiteName());

            if (bookElements.isEmpty()) {
                logger.warn("未找到书籍元素: {}", website.getWebsiteUrl());
                return Mono.just(false);
            }

            // 限制一次处理的书籍数量，防止请求过多
            int limit = Math.min(bookElements.size(), 200);
            logger.info("限制处理书籍数量: {}", limit);

            List<Mono<Boolean>> saveTasks = new ArrayList<>();

            for (int i = 0; i < limit; i++) {
                Element bookElement = bookElements.get(i);
                Book book = new Book();
                book.setWebsiteId(website.getId());
                book.setBookName(extractBookName(bookElement));
                book.setBookSummary(extractDescription(bookElement));
                // 封面URL暂时存放在bookSummary的额外字段中
                book.setBookUrl(extractBookUrl(bookElement, website.getWebsiteUrl()));
                book.setCreateTime(LocalDateTime.now());
                book.setModifyTime(LocalDateTime.now());
                // 设置创建用户信息
                book.setCreateUserId(1L);
                book.setCreateUserName("system");
                book.setModifyUserId(1L);
                book.setModifyUserName("system");
                // 验证必要字段
                if (book.getBookName() == null || book.getBookUrl() == null) {
                    logger.warn("跳过无效书籍数据: name={}, url={}", book.getBookName(), book.getBookUrl());
                    continue;
                }

                logger.info("准备处理书籍: {}, URL: {}", book.getBookName(), book.getBookUrl());
                // 保存书籍并发送章节任务
                saveTasks.add(saveBookAndSendChaptersWithCheck(book, website));
            }

            if (saveTasks.isEmpty()) {
                logger.warn("没有有效的书籍任务: {}", website.getWebsiteUrl());
                return Mono.just(false);
            }

            // 并行执行所有保存任务
            return reactor.core.publisher.Flux.fromIterable(saveTasks)
                    .flatMap(task -> task, 3) // 限制并发数为3，避免请求过多
                    .collectList()
                    .doOnSuccess(results -> {
                        logger.info("所有书籍保存任务完成，结果数量: {}", results.size());
                    })
                    .doOnError(error -> {
                        logger.error("执行书籍保存任务时出错: {}", error.getMessage(), error);
                    })
                    .map(results -> !results.isEmpty() && results.stream().anyMatch(result -> result))
                    .onErrorReturn(false);

        } catch (Exception e) {
            logger.error("解析书籍失败: {}", e.getMessage(), e);
            return Mono.just(false);
        }
    }
    
    /**
     * 检查重复并保存书籍（优先从Redis检查），无论是否新增都获取章节数据
     */
    private Mono<Boolean> saveBookAndSendChaptersWithCheck(Book book, Website website) {
        // 构建Redis键
        String bookNameKey = BOOK_NAME_KEY_PREFIX + website.getId() + ":" + book.getBookName();
        
        // 先从Redis检查书籍是否存在
        return reactiveRedisTemplate.opsForValue().get(bookNameKey)
            .flatMap(existingBookId -> {
                logger.info("Redis中检测到书籍已存在: {}，网站: {}", 
                        book.getBookName(), website.getWebsiteName());
                // 获取现有书籍信息并获取章节数据
                return bookService.findById(Long.valueOf(existingBookId.toString()))
                    .flatMap(existingBook -> {
                        // 无论书籍是否新增，都获取章节数据
                        return fetchAndParseChapters(existingBook, website);
                    });
            })
            .switchIfEmpty(Mono.defer(() -> {
                // Redis中不存在，再查询数据库确认
                logger.info("Redis中未找到书籍，查询数据库: {}，网站: {}", 
                        book.getBookName(), website.getWebsiteName());
                return bookService.findByWebsiteIdAndBookName(website.getId(), book.getBookName())
                    .flatMap(existingBook -> {
                        logger.info("数据库中检测到书籍已存在: {}，网站: {}", 
                                book.getBookName(), website.getWebsiteName());
                        // 数据库中存在，将其缓存到Redis
                        return reactiveRedisTemplate.opsForValue()
                            .set(bookNameKey, existingBook.getId())
                            .then(reactiveRedisTemplate.opsForValue()
                                .set("book:" + existingBook.getId(), existingBook))
                            .then(fetchAndParseChapters(existingBook, website));
                    })
                    .switchIfEmpty(Mono.defer(() -> {
                        logger.info("准备保存新书籍: {}，网站: {}", 
                                book.getBookName(), website.getWebsiteName());
                        return saveBookAndSendChapters(book, website);
                    }));
            }));
    }
    
    /**
     * 获取并解析章节数据
     */
    private Mono<Boolean> fetchAndParseChapters(Book book, Website website) {
        logger.info("准备获取书籍章节数据: {}，网站: {}", book.getBookName(), website.getWebsiteName());
        return fetchHtml(book.getBookUrl())
            .flatMap(chapterListHtml -> {
                logger.info("获取章节列表成功，准备解析章节: {}", book.getBookName());
                // 解析章节并发送任务
                parseChaptersAndSendTasks(chapterListHtml, book, website);
                return Mono.just(true);
            })
            .onErrorResume(e -> {
                logger.error("获取章节列表失败: {}", e.getMessage(), e);
                return Mono.just(true); // 继续处理其他书籍
            });
    }

    /**
     * 保存书籍并发送章节任务
     */
    private Mono<Boolean> saveBookAndSendChapters(Book book, Website website) {
        logger.info("准备保存书籍: {}", book.getBookName());
        // 直接保存书籍（实际项目中应该先查询是否存在）
        return bookService.save(book)
                .publishOn(Schedulers.boundedElastic()) // 确保在正确的线程上执行IO操作
                .doOnSuccess(savedBook -> {
                    logger.info("书籍保存成功: {}, ID: {}", savedBook.getBookName(), savedBook.getId());
                })
                .doOnError(error -> {
                    logger.error("书籍保存失败: {}, 错误: {}", book.getBookName(), error.getMessage(), error);
                })
                .flatMap(savedBook -> {
                    // 调用通用方法获取并解析章节数据
                    return fetchAndParseChapters(savedBook, website);
                })
                .onErrorResume(e -> {
                    logger.error("保存书籍并发送章节任务失败: {}", e.getMessage(), e);
                    return Mono.just(false);
                });
    }

    /**
     * 解析章节并发送任务
     */
    private void parseChaptersAndSendTasks(String html, Book book, Website website) {
        if (html == null || html.isEmpty()) {
            logger.warn("章节列表HTML为空，跳过解析: {}", book.getBookName());
            return;
        }

        try {
            Document doc = Jsoup.parse(html);

            // 通用的章节列表解析示例
            Elements chapterElements = doc.select(".panel-chapterlist a, .chapter-item a, .volume-list a");
            logger.info("找到章节数量: {}，书籍: {}", chapterElements.size(), book.getBookName());
            
            int orderId = 1;
            for (Element chapterElement : chapterElements) {
                String chapterName = chapterElement.text().trim();
                String chapterUrl = chapterElement.absUrl("href");

                if (!chapterName.isEmpty() && !chapterUrl.isEmpty()) {
                    ChapterTask task = new ChapterTask();
                    task.setBookId(book.getId());
                    task.setChapterName(chapterName);
                    task.setChapterUrl(chapterUrl);
                    task.setChapterOrderId(orderId++);
                    task.setWebsiteId(website.getId());
                    task.setWebsiteUrl(website.getWebsiteUrl());

                    // 发送到RabbitMQ
                    try {
                        logger.info("准备发送章节任务: 书籍ID={}, 章节名称={}", 
                                task.getBookId(), task.getChapterName());
                        rabbitMQService.sendChapterTask(task);
                        logger.info("章节任务发送成功: 书籍ID={}, 章节名称={}", 
                                task.getBookId(), task.getChapterName());
                    } catch (Exception e) {
                        logger.error("章节任务发送失败: 书籍ID={}, 章节名称={}, 错误: {}", 
                                task.getBookId(), task.getChapterName(), e.getMessage(), e);
                        // 继续发送其他章节，不中断循环
                    }

                    // 避免发送过快，加入短暂延迟
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }

            logger.info("书籍章节任务发送完成: {}，共{}章", book.getBookName(), chapterElements.size());

        } catch (Exception e) {
            logger.error("解析章节失败: {}，错误: {}", book.getBookName(), e.getMessage(), e);
        }
    }

    // 以下是通用的提取方法，实际使用时需要根据网站结构调整

    private String extractBookName(Element element) {
        return element.attribute("title").getValue().trim();
    }

    // 由于Book类中没有author字段，暂时注释掉
    /*private String extractAuthor(Element element) {
        Elements authorElements = element.select(".book-author, .author, .writer");
        return authorElements.isEmpty() ? null : authorElements.first().text().trim();
    }*/

    private String extractDescription(Element element) {
        Elements descElements = element.select(".glyphicon,.glyphicon-fire");
        return descElements.isEmpty() ? null : descElements.first().text().trim();
    }

    private String extractCoverUrl(Element element) {
        Elements coverElements = element.select(".book-cover img, .cover img, img[src*=cover]");
        return coverElements.isEmpty() ? null : coverElements.first().absUrl("src");
    }

    private String extractBookUrl(Element element, String baseUrl) {
        Elements urlElements = element.select("a[href]");
        if (urlElements.isEmpty()) {
            return null;
        }
        String url = urlElements.first().absUrl("href");
        return url.isEmpty() ? null : url;
    }
}