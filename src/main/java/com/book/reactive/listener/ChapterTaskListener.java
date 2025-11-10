package com.book.reactive.listener;

import com.book.reactive.model.Chapter;
import com.book.reactive.model.ChapterTask;
import com.book.reactive.service.ChapterService;
import com.book.reactive.util.StringUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import javax.annotation.PostConstruct;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuples;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
    
    private final Sinks.Many<ChapterTask> taskSink;
    private final Map<String, Boolean> processedFeatureCodes = new ConcurrentHashMap<>();
    
    @Value("${batch.size:100}")
    private int batchSize;
    
    @Value("${batch.timeout:5}")
    private int batchTimeoutSeconds;

    private static final String CHAPTER_URL_KEY_PREFIX = "chapter:url:";
    private static final String CHAPTER_FEATURE_CODE_KEY_PREFIX = "chapter:feature:";

    @Autowired
    public ChapterTaskListener(ObjectMapper objectMapper, ChapterService chapterService, ReactiveRedisTemplate<String, Object> reactiveRedisTemplate, StringUtil stringUtil) {
        this.objectMapper = objectMapper;
        this.chapterService = chapterService;
        this.reactiveRedisTemplate = reactiveRedisTemplate;
        this.stringUtil = stringUtil;
        this.webClient = WebClient.builder()
                .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(50 * 1024 * 1024)) // 50MB
                .build();
        
        // 创建一个可多播的Sink
        this.taskSink = Sinks.many().unicast().onBackpressureBuffer();
    }
    
    @PostConstruct
    public void init() {
        // 确保在所有属性注入完成后再启动批处理流程
        startBatchProcessing();
    }

    /**
     * 监听章节处理队列 - 批量模式
     */
    @RabbitListener(queues = "${chapter.queue.name}", containerFactory = "batchRabbitListenerContainerFactory")
    public void processChapterTask(List<ChapterTask> chapterTasks) {
        logger.info("接收到批量章节处理任务: 任务数量={}", chapterTasks.size());

        try {
            // 将批量任务发送到Sink中进行处理
            chapterTasks.forEach(task -> {
                try {
                    // 使用emitNext并指定RETRY_NON_SERIALIZED模式，确保在多线程环境下能正确处理
                    Sinks.EmitResult result = taskSink.tryEmitNext(task);
                    if (result.isFailure()) {
                        logger.error("任务入队失败: {} - {}, 原因: {}", 
                            task.getBookId(), task.getChapterName(), result);
                    }
                } catch (Exception e) {
                    logger.error("任务入队异常: {} - {}, 原因: {}", 
                        task.getBookId(), task.getChapterName(), e.getMessage(), e);
                }
            });
        } catch (Exception e) {
            logger.error("批量任务处理失败: {}", e.getMessage(), e);
            // 这里可以根据需要进行重试或发送到死信队列
        }
    }
    
    /**
     * 启动批处理流程
     */
    private void startBatchProcessing() {
        // 确保超时时间为正数，防止出现Timeout period must be strictly positive异常
        int safeTimeoutSeconds = Math.max(5, batchTimeoutSeconds);
        // 确保批次大小为正数，防止出现maxSize must be strictly positive异常
        int safeBatchSize = Math.max(500, batchSize);
        
        // 按照批次大小和超时时间收集任务
        taskSink.asFlux()
            .bufferTimeout(safeBatchSize, Duration.ofSeconds(safeTimeoutSeconds))
            .doOnNext(batch -> logger.info("处理批次任务: 数量={}", batch.size()))
            .flatMap(this::processBatchTasks)
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe(
                result -> logger.info("批次处理完成: 成功={}", result),
                error -> logger.error("批次处理异常: {}", error.getMessage(), error)
            );
    }

    /**
     * 处理批次任务
     */
    private Mono<Boolean> processBatchTasks(List<ChapterTask> tasks) {
        // 1. 批量构建Redis键并进行重复检查
        List<String> redisKeys = new ArrayList<>();
        Map<String, ChapterTask> keyToTask = new ConcurrentHashMap<>();
        
        // 收集所有需要检查的Redis键
        tasks.forEach(task -> {
            String urlKey = CHAPTER_URL_KEY_PREFIX + task.getBookId() + ":" + task.getChapterUrl();
            redisKeys.add(urlKey);
            keyToTask.put(urlKey, task);
        });
        
        // 2. 批量查询Redis，检查章节是否已存在
        return Flux.fromIterable(redisKeys)
            .flatMap(key -> reactiveRedisTemplate.opsForValue().get(key)
                .map(existingId -> Tuples.of(key, true))  // 存在
                .defaultIfEmpty(Tuples.of(key, false))    // 不存在
                .timeout(Duration.ofSeconds(5))
            )
            .collectMap(tuple -> tuple.getT1(), tuple -> tuple.getT2())
            .flatMap(existingKeysMap -> {
                // 3. 筛选出不存在的任务进行处理
                List<ChapterTask> newTasks = tasks.stream()
                    .filter(task -> {
                        String urlKey = CHAPTER_URL_KEY_PREFIX + task.getBookId() + ":" + task.getChapterUrl();
                        return !existingKeysMap.getOrDefault(urlKey, false);
                    })
                    .collect(Collectors.toList());
                
                logger.info("批次中需要新增的章节数量: {}", newTasks.size());
                
                // 4. 批量获取和处理章节内容
                return processNewChapters(newTasks);
            });
    }
    
    /**
     * 处理新的章节任务
     */
    private Mono<Boolean> processNewChapters(List<ChapterTask> tasks) {
        return Flux.<ChapterTask>fromIterable(tasks)
            // 并行获取章节内容
            .flatMap(task -> fetchChapterContent(task)
                .timeout(Duration.ofSeconds(60))
                .onErrorResume(error -> {
                    logger.error("获取章节内容失败: {} - {}", task.getChapterName(), error.getMessage(), error);
                    return Mono.<reactor.util.function.Tuple2<ChapterTask, String>>empty();
                })
            )
            .filter(tuple -> tuple != null && tuple.getT2() != null && !tuple.getT2().isEmpty())
            .flatMapSequential(tuple -> {
                ChapterTask task = tuple.getT1();
                String content = tuple.getT2();
                
                // 生成特征码并检查重复（基于内容）
                String featureCode = stringUtil.extractFeatureCode(content);
                String featureCodeKey = CHAPTER_FEATURE_CODE_KEY_PREFIX + featureCode;
                
                // 先在内存中检查，避免重复处理
                if (processedFeatureCodes.containsKey(featureCode)) {
                    logger.info("内存中检测到章节内容重复: {} - {}", task.getChapterName(), featureCode);
                    return Mono.<Chapter>empty();
                }
                
                // 标记为已处理
                processedFeatureCodes.put(featureCode, true);
                
                // 检查Redis中是否存在特征码
                return reactiveRedisTemplate.opsForValue().get(featureCodeKey)
                    .flatMap(existingId -> {
                        logger.info("Redis中检测到章节内容重复: {} - {}", task.getChapterName(), featureCode);
                        // 缓存URL键，避免后续重复处理
                        String urlKey = CHAPTER_URL_KEY_PREFIX + task.getBookId() + ":" + task.getChapterUrl();
                        return reactiveRedisTemplate.opsForValue().set(urlKey, existingId)
                            .then(Mono.<Chapter>empty());
                    })
                    .switchIfEmpty(Mono.defer(() -> {
                        // 不存在，创建Chapter对象
                        return createChapterFromTask(task, content, featureCode);
                    }));
            })
            // 批量保存章节
            .collectList()
            .flatMap(chapters -> {
                if (chapters.isEmpty()) {
                    logger.info("批次中无可保存的新章节");
                    return Mono.just(true);
                }
                
                logger.info("准备批量保存章节: 数量={}", chapters.size());
                return chapterService.saveAll(Flux.fromIterable(chapters))
                    .count()
                    .doOnSuccess(savedCount -> logger.info("批量保存完成: 成功={}个章节", savedCount))
                    .map(count -> count > 0);
            });
    }
    
    /**
     * 获取章节内容
     */
    private Mono<reactor.util.function.Tuple2<ChapterTask, String>> fetchChapterContent(ChapterTask task) {
        return webClient.get()
            .uri(task.getChapterUrl())
            .retrieve()
            .bodyToMono(String.class)
            .map(html -> {
                String content = parseChapterContent(html);
                logger.info("章节内容解析完成: {}，内容长度={}", task.getChapterName(), content.length());
                return Tuples.of(task, content);
            })
            .subscribeOn(Schedulers.boundedElastic());
    }
    
    /**
     * 从任务创建章节对象
     */
    private Mono<Chapter> createChapterFromTask(ChapterTask task, String content, String featureCode) {
        Chapter chapter = new Chapter();
        chapter.setBookId(task.getBookId());
        chapter.setChapterUrl(task.getChapterUrl());
        chapter.setChapterName(task.getChapterName());
        chapter.setChapterOrderId(task.getChapterOrderId());
        chapter.setChapterTxt(content);
        chapter.setFeatureCode(featureCode);
        chapter.setCreateUserId(1L);
        chapter.setCreateUserName("system");
        chapter.setModifyUserId(1L);
        chapter.setModifyUserName("system");
        
        return Mono.just(chapter);
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
     * 保存章节到数据库（优先从Redis检查） - 已被批量处理取代，保留作为备用
     */
    private void saveChapterToDatabase(ChapterTask chapterTask, String content) {
        logger.info("准备保存章节: {}，书籍ID={}", chapterTask.getChapterName(), chapterTask.getBookId());
        
        // 构建Redis键
        String chapterUrlKey = CHAPTER_URL_KEY_PREFIX + chapterTask.getBookId() + ":" + chapterTask.getChapterUrl();
                String featureCode = stringUtil.extractFeatureCode(content);
        String chapterFeatureCodeKey = "chapter:feature:" + featureCode;  

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
                        // .then(reactiveRedisTemplate.opsForValue()
                        //     .set("chapter:" + existingChapter.getId(), existingChapter))
                        // .timeout(Duration.ofSeconds(10)) // 添加超时控制
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
                            return reactiveRedisTemplate.opsForValue().set(chapterFeatureCodeKey, savedChapter.getId())
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

                        return reactiveRedisTemplate.opsForValue().set(chapterFeatureCodeKey, savedChapter.getId())
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
                        
                        return reactiveRedisTemplate.opsForValue().set(chapterFeatureCodeKey, savedChapter.getId())
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