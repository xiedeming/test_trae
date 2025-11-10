package com.book.reactive.service;

import com.book.reactive.model.ChapterTask;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * RabbitMQ消息发送服务
 */
@Service
public class RabbitMQService {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMQService.class);

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Value("${chapter.exchange.name}")
    private String chapterExchangeName;

    @Value("${chapter.routing.key}")
    private String chapterRoutingKey;

    /**
     * 同步发送章节处理任务到队列
     */
    public void sendChapterTask(ChapterTask chapterTask) {
        try {
            rabbitTemplate.convertAndSend(chapterExchangeName, chapterRoutingKey, chapterTask);
            logger.info("章节任务发送成功: 书籍ID={}, 章节名称={}", 
                    chapterTask.getBookId(), chapterTask.getChapterName());
        } catch (Exception e) {
            logger.error("章节任务发送失败: {}", e.getMessage(), e);
            throw e;
        }
    }

    /**
     * 异步发送章节处理任务到队列
     */
    @Async
    public CompletableFuture<Boolean> sendChapterTaskAsync(ChapterTask chapterTask) {
        try {
            CorrelationData correlationData = new CorrelationData(UUID.randomUUID().toString());
            rabbitTemplate.convertAndSend(chapterExchangeName, chapterRoutingKey, chapterTask, correlationData);
            logger.info("异步章节任务发送成功: 书籍ID={}, 章节名称={}", 
                    chapterTask.getBookId(), chapterTask.getChapterName());
            return CompletableFuture.completedFuture(true);
        } catch (Exception e) {
            logger.error("异步章节任务发送失败: 书籍ID={}, 章节名称={}, 错误: {}", 
                    chapterTask.getBookId(), chapterTask.getChapterName(), e.getMessage(), e);
            return CompletableFuture.completedFuture(false);
        }
    }

    /**
     * Reactor风格的异步发送方法
     */
    public Mono<Boolean> sendChapterTaskReactive(ChapterTask chapterTask) {
        return Mono.fromFuture(sendChapterTaskAsync(chapterTask));
    }
}