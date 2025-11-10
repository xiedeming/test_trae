package com.book.reactive.listener;

import com.book.reactive.model.Chapter;
import com.book.reactive.model.ChapterTask;
import com.book.reactive.service.ChapterService;
import com.book.reactive.util.StringUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;

/**
 * 章节任务监听器，用于处理RabbitMQ队列中的章节任务
 */
@Component
public class ChapterTaskListener {

    private static final Logger logger = LoggerFactory.getLogger(ChapterTaskListener.class);

    private final WebClient webClient;
    private final ObjectMapper objectMapper;
    private final ChapterService chapterService;
    private final ReactiveRedisTemplate<String, Object> reactiveRedisTemplate;
    private final StringUtil stringUtil;

    private static final String CHAPTER_URL_KEY_PREFIX = "chapter:url:";

    @Autowired
    public ChapterTaskListener(ObjectMapper objectMapper, ChapterService chapterService, ReactiveRedisTemplate<String, Object> reactiveRedisTemplate, StringUtil stringUtil) {
        this.objectMapper = objectMapper;
        this.chapterService = chapterService;
        this.reactiveRedisTemplate = reactiveRedisTemplate;
        this.stringUtil = stringUtil;
        this.webClient = WebClient.builder()
                .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(5 * 1024 * 1024)) // 5MB
                .build();
    }

    /**
     * 监听章节处理队列
     */
    @RabbitListener(queues = "${chapter.queue.name}")
    public void processChapterTask(ChapterTask chapterTask) {
        logger.info("接收到章节处理任务: 书籍ID={}, 章节名称={}, 章节URL={}",
                chapterTask.getBookId(), chapterTask.getChapterName(), chapterTask.getChapterUrl());

        try {
            // 处理章节任务
            handleChapterTask(chapterTask);
        } catch (Exception e) {
            logger.error("处理章节任务失败: {} - {}", chapterTask.getChapterName(), e.getMessage(), e);
            // 这里可以根据需要进行重试或发送到死信队列
            throw e; // 抛出异常会触发重试或死信队列
        }
    }

    /**
     * 处理单个章节任务
     */
    private void handleChapterTask(ChapterTask chapterTask) {
        // 模拟处理延迟，避免处理过快
        // try {
        //     Thread.sleep(1000);
        // } catch (InterruptedException e) {
        //     Thread.currentThread().interrupt();
        // }

        // 使用WebClient异步获取章节内容
        webClient.get()
                .uri(chapterTask.getChapterUrl())
                .retrieve()
                .bodyToMono(String.class)
                .timeout(Duration.ofSeconds(60))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                        html -> {
                            try {
                                // 解析章节内容
                                String chapterContent = parseChapterContent(html);
                                logger.info("章节内容解析完成: {}，内容长度={}",
                                        chapterTask.getChapterName(), chapterContent.length());

                                // 这里可以调用章节服务保存章节内容
                                saveChapterToDatabase(chapterTask, chapterContent);

                            } catch (Exception e) {
                                logger.error("解析章节内容失败: {}", e.getMessage(), e);
                            }
                        },
                        error -> {
                            logger.error("获取章节页面失败: {} - {}", 
                                    chapterTask.getChapterUrl(), error.getMessage());
                        }
                );
    }

    /**
     * 解析章节内容
     */
    private String parseChapterContent(String html) {
        if (html == null || html.isEmpty()) {
            return "";
        }

        try {
            Document doc = Jsoup.parse(html);
            
            // 通用的章节内容解析示例
            // 实际使用时需要根据具体网站的HTML结构进行调整
            Elements contentElements = doc.select(
                    "#htmlContent");
            
            if (contentElements.isEmpty()) {
                // 如果没有找到内容元素，尝试清理整个文档
                // 移除脚本和样式
                doc.select("script, style, .advert, .footer, .header").remove();
                // 获取body文本
                return doc.body().text();
            }

            Element contentElement = contentElements.first();
            // 清理内容中的广告、脚本等
            contentElement.select("script, style, .advert, .footer").remove();
            // 获取清理后的文本
            return contentElement.text().trim();

        } catch (Exception e) {
            logger.error("解析HTML失败: {}", e.getMessage());
            return "";
        }
    }

    /**
     * 保存章节到数据库（优先从Redis检查）
     */
    private void saveChapterToDatabase(ChapterTask chapterTask, String content) {
        logger.info("准备保存章节: {}，书籍ID={}", chapterTask.getChapterName(), chapterTask.getBookId());
        
        // 构建Redis键
        String chapterUrlKey = CHAPTER_URL_KEY_PREFIX + chapterTask.getBookId() + ":" + chapterTask.getChapterUrl();
        String chapterFeatureCodeKey = "chapter:feature:" + stringUtil.extractFeatureCode(content);
        
        // 先从Redis检查章节是否存在（使用特征码）
        reactiveRedisTemplate.opsForValue().get(chapterFeatureCodeKey)
            .subscribeOn(Schedulers.boundedElastic())
            .timeout(Duration.ofSeconds(30)) // 添加超时控制
            .log("Redis查询日志") // 添加响应式操作日志
            .flatMap(existingChapterId -> {
                logger.info("Redis中检测到章节已存在(基于特征码): {}，书籍ID={}", 
                    chapterTask.getChapterName(), chapterTask.getBookId());
                // Redis中存在，表示章节已存在，跳过保存
                return Mono.just(existingChapterId);
            })
            .switchIfEmpty(Mono.defer(() -> {
                // Redis中不存在，再查询数据库确认
                logger.info("Redis中未找到章节，查询数据库: {}，书籍ID={}", 
                    chapterTask.getChapterName(), chapterTask.getBookId());
                return chapterService.findByBookIdAndChapterUrl(chapterTask.getBookId(), chapterTask.getChapterUrl())
                .timeout(Duration.ofSeconds(30)) // 添加超时控制
                .log("数据库查询日志") // 添加响应式操作日志
                .flatMap(existingChapter -> {
                    if (existingChapter != null) {
                        logger.info("数据库中检测到章节已存在: {}，书籍ID={}", 
                            chapterTask.getChapterName(), chapterTask.getBookId());
                        // 数据库中存在，将其缓存到Redis（同时缓存特征码和URL索引）
                    return reactiveRedisTemplate.opsForValue()
                        .set("chapter:feature:" + existingChapter.getFeatureCode(), existingChapter.getId())
                        .timeout(Duration.ofSeconds(10)) // 添加超时控制
                        .then(reactiveRedisTemplate.opsForValue()
                            .set(chapterUrlKey, existingChapter.getId()))
                        .timeout(Duration.ofSeconds(10)) // 添加超时控制
                        .then(reactiveRedisTemplate.opsForValue()
                            .set("chapter:" + existingChapter.getId(), existingChapter))
                        .timeout(Duration.ofSeconds(10)) // 添加超时控制
                        .then(Mono.just(existingChapter.getId()));
                    }
                    
                    logger.info("准备创建新章节: {}，书籍ID={}", chapterTask.getChapterName(), chapterTask.getBookId());
                    // 创建Chapter对象
                    Chapter chapter = new Chapter();
                    chapter.setBookId(chapterTask.getBookId());
                    chapter.setChapterUrl(chapterTask.getChapterUrl());
                    chapter.setChapterName(chapterTask.getChapterName());
                    chapter.setChapterOrderId(chapterTask.getChapterOrderId());
                    chapter.setChapterTxt(content);
                    // 生成并设置章节特征码
                    String featureCode = stringUtil.extractFeatureCode(content);
                    chapter.setFeatureCode(featureCode);
                    logger.info("章节特征码生成完成: {}，特征码={}", chapterTask.getChapterName(), featureCode);
                    
                    // 设置创建用户信息
                    chapter.setCreateUserId(1L);
                    chapter.setCreateUserName("system");
                    chapter.setModifyUserId(1L);
                    chapter.setModifyUserName("system");
                    
                    // 保存新章节
                    logger.info("调用章节服务保存新章节: {}", chapterTask.getChapterName());
                    return chapterService.save(chapter)
                        .timeout(Duration.ofSeconds(60)) // 添加超时控制
                        .log("章节保存日志") // 添加响应式操作日志
                        .flatMap(savedChapter -> {
                            logger.info("章节保存成功并更新Redis缓存: {}，章节ID={}", 
                                chapterTask.getChapterName(), savedChapter.getId());
                            // 缓存章节到Redis（使用特征码作为主要缓存键）
                            String chapterKey = "chapter:" + savedChapter.getId();

                            return reactiveRedisTemplate.opsForValue().set(chapterKey, savedChapter)
                                .timeout(Duration.ofSeconds(10))
                                .then(reactiveRedisTemplate.opsForValue().set(chapterFeatureCodeKey, savedChapter.getId()))
                                .timeout(Duration.ofSeconds(10))
                                .then(reactiveRedisTemplate.opsForValue().set(chapterUrlKey, savedChapter.getId()))
                                .timeout(Duration.ofSeconds(10))
                                .then(Mono.just(savedChapter.getId()));
                        });
                })
                .switchIfEmpty(Mono.defer(() -> {
                    logger.warn("数据库查询返回null，准备创建新章节: {}", chapterTask.getChapterName());
                    // 创建Chapter对象
                    Chapter chapter = new Chapter();
                    chapter.setBookId(chapterTask.getBookId());
                    chapter.setChapterUrl(chapterTask.getChapterUrl());
                    chapter.setChapterName(chapterTask.getChapterName());
                    chapter.setChapterOrderId(chapterTask.getChapterOrderId());
                    chapter.setChapterTxt(content);
                    // 生成并设置特征码
                    String featureCode = stringUtil.extractFeatureCode(content);
                    chapter.setFeatureCode(featureCode);
                    
                    // 设置创建用户信息
                    chapter.setCreateUserId(1L);
                    chapter.setCreateUserName("system");
                    chapter.setModifyUserId(1L);
                    chapter.setModifyUserName("system");
                    
                    // 保存新章节
                    return Mono.just(chapter);
                }).flatMap(chapter -> chapterService.save(chapter)
                    .timeout(Duration.ofSeconds(60))
                    .log("章节保存日志(备用)")
                    .flatMap(savedChapter -> {
                        logger.info("章节保存成功(备用路径): {}，章节ID={}，特征码={}", 
                            chapterTask.getChapterName(), savedChapter.getId(), savedChapter.getFeatureCode());
                        // 缓存章节到Redis（使用特征码作为主要缓存键）
                        String chapterKey = "chapter:" + savedChapter.getId();
                        return reactiveRedisTemplate.opsForValue().set(chapterKey, savedChapter)
                            .timeout(Duration.ofSeconds(10))
                            .then(reactiveRedisTemplate.opsForValue().set(chapterFeatureCodeKey, savedChapter.getId()))
                            .timeout(Duration.ofSeconds(10))
                            .then(reactiveRedisTemplate.opsForValue().set(chapterUrlKey, savedChapter.getId()))
                            .timeout(Duration.ofSeconds(10))
                            .then(Mono.just(savedChapter.getId()));
                    })));
            }))
            .doOnError(error -> {
                logger.error("章节处理过程中发生错误: {} - {}", 
                    chapterTask.getChapterName(), error.getMessage(), error);
            })
            .onErrorResume(error -> {
                logger.error("章节处理失败，尝试备用保存路径: {}", chapterTask.getChapterName());
                // 备用保存路径，直接保存到数据库
                Chapter chapter = new Chapter();
                chapter.setBookId(chapterTask.getBookId());
                chapter.setChapterUrl(chapterTask.getChapterUrl());
                chapter.setChapterName(chapterTask.getChapterName());
                chapter.setChapterOrderId(chapterTask.getChapterOrderId());
                chapter.setChapterTxt(content);
                // 生成并设置特征码
                String featureCode = stringUtil.extractFeatureCode(content);
                chapter.setFeatureCode(featureCode);
                chapter.setCreateUserId(1L);
                chapter.setCreateUserName("system");
                chapter.setModifyUserId(1L);
                chapter.setModifyUserName("system");
                
                return chapterService.save(chapter)
                    .timeout(Duration.ofSeconds(60))
                    .flatMap(savedChapter -> {
                        logger.info("备用路径保存成功: {}，章节ID={}，特征码={}", 
                            chapterTask.getChapterName(), savedChapter.getId(), savedChapter.getFeatureCode());
                        // 缓存章节到Redis（使用特征码作为主要缓存键）
                        String chapterKey = "chapter:" + savedChapter.getId();
                        
                        return reactiveRedisTemplate.opsForValue().set(chapterKey, savedChapter)
                            .timeout(Duration.ofSeconds(10))
                            .then(reactiveRedisTemplate.opsForValue().set(chapterFeatureCodeKey, savedChapter.getId()))
                            .timeout(Duration.ofSeconds(10))
                            .then(reactiveRedisTemplate.opsForValue().set(chapterUrlKey, savedChapter.getId()))
                            .timeout(Duration.ofSeconds(10))
                            .then(Mono.just(savedChapter.getId()));
                    })
                    .onErrorResume(e -> {
                        logger.error("备用路径保存也失败: {} - {}", 
                            chapterTask.getChapterName(), e.getMessage(), e);
                        return Mono.just(-1L); // 返回特殊值表示处理失败
                    });
            })
            .subscribe(
                chapterId -> {
                    logger.info("章节处理完成: {}，章节ID={}", 
                        chapterTask.getChapterName(), chapterId);
                },
                error -> {
                    logger.error("章节保存失败(最终): {} - {}", 
                        chapterTask.getChapterName(), error.getMessage(), error);
                }
            );
    }
}