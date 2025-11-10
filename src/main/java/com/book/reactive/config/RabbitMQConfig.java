package com.book.reactive.config;

import org.springframework.amqp.core.*;
import org.springframework.amqp.support.converter.DefaultClassMapper;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
/**
 * RabbitMQ配置类
 */
@Configuration
public class RabbitMQConfig {

    @Value("${chapter.queue.name}")
    private String chapterQueueName;

    @Value("${chapter.exchange.name}")
    private String chapterExchangeName;

    @Value("${chapter.routing.key}")
    private String chapterRoutingKey;

    /**
     * 创建章节处理队列
     */
    @Bean
    public Queue chapterQueue() {
        return QueueBuilder.durable(chapterQueueName)
                .withArgument("x-dead-letter-exchange", "chapter_dlx_exchange")
                .withArgument("x-dead-letter-routing-key", "chapter.dlx")
                .build();
    }

    /**
     * 创建交换机
     */
    @Bean
    public DirectExchange chapterExchange() {
        return new DirectExchange(chapterExchangeName);
    }

    /**
     * 绑定队列和交换机
     */
    @Bean
    public Binding chapterBinding(Queue chapterQueue, DirectExchange chapterExchange) {
        return BindingBuilder.bind(chapterQueue).to(chapterExchange).with(chapterRoutingKey);
    }

    /**
     * 创建死信交换机
     */
    @Bean
    public DirectExchange chapterDlxExchange() {
        return new DirectExchange("chapter_dlx_exchange");
    }

    /**
     * 创建死信队列
     */
    @Bean
    public Queue chapterDlxQueue() {
        return QueueBuilder.durable("chapter_dlx_queue").build();
    }

    /**
     * 绑定死信队列和死信交换机
     */
    @Bean
    public Binding chapterDlxBinding(Queue chapterDlxQueue, DirectExchange chapterDlxExchange) {
        return BindingBuilder.bind(chapterDlxQueue).to(chapterDlxExchange).with("chapter.dlx");
    }

    /**
     * 配置消息转换器，允许反序列化ChapterTask类并正确处理日期格式
     */
    @Bean
    public Jackson2JsonMessageConverter messageConverter() {
        // 配置ObjectMapper
        ObjectMapper objectMapper = new ObjectMapper();
        
        // 添加JavaTimeModule并配置LocalDateTime序列化格式
        JavaTimeModule timeModule = new JavaTimeModule();
        // 配置LocalDateTime的序列化格式为yyyy-MM-dd HH:mm:ss
        timeModule.addSerializer(LocalDateTime.class, 
            new LocalDateTimeSerializer(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        objectMapper.registerModule(timeModule);
        
        // 禁用日期作为时间戳
        objectMapper.disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        
        // 设置非空字段才序列化
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        
        objectMapper.findAndRegisterModules();
        
        Jackson2JsonMessageConverter converter = new Jackson2JsonMessageConverter(objectMapper);
        DefaultClassMapper classMapper = new DefaultClassMapper();
        classMapper.setTrustedPackages("com.book.reactive.model");
        converter.setClassMapper(classMapper);
        return converter;
    }
    
    /**
     * 配置批量监听器容器工厂，用于批量消费MQ消息
     */
    @Bean(name = "batchRabbitListenerContainerFactory")
    public org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory<?> batchRabbitListenerContainerFactory(org.springframework.amqp.rabbit.connection.ConnectionFactory connectionFactory,
                                                                                 Jackson2JsonMessageConverter messageConverter) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(messageConverter);
        
        // 配置批量消费参数
        factory.setBatchListener(true); // 启用批量监听器
        factory.setBatchSize(100); // 每次批量处理的消息数量上限
        factory.setConsumerBatchEnabled(true); // 启用消费者批量处理
        factory.setAcknowledgeMode(AcknowledgeMode.MANUAL); // 手动确认
        
        // 配置并发参数
        factory.setConcurrentConsumers(5); // 并发消费者数量
        factory.setMaxConcurrentConsumers(10); // 最大并发消费者数量
        
        // 设置预取数量，提高吞吐量
        factory.setPrefetchCount(200);
        
        return factory;
    }
    
    /**
     * 配置默认的监听器容器工厂
     */
    @Bean(name = "rabbitListenerContainerFactory")
    public RabbitListenerContainerFactory<?> defaultRabbitListenerContainerFactory(ConnectionFactory connectionFactory,
                                                                                  Jackson2JsonMessageConverter messageConverter) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(messageConverter);
        factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        factory.setConcurrentConsumers(3);
        factory.setMaxConcurrentConsumers(5);
        factory.setPrefetchCount(100);
        return factory;
    }
}