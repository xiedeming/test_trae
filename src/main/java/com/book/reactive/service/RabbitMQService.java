package com.book.reactive.service;

import com.book.reactive.model.ChapterTask;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
     * 发送章节处理任务到队列
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
}