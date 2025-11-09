package com.book.reactive.repository;

import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;

import com.book.reactive.model.Book;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface BookRepository extends ReactiveCrudRepository<Book, Long> {
    // 根据书名模糊查询的分页方法
    Flux<Book> findByBookNameContaining(String bookName, Pageable pageable);
    
    // 根据书籍名称和网站ID精确查询
    Mono<Book> findByWebsiteIdAndBookName(Long websiteId, String bookName);
    
    // 统计符合条件的记录数
    Mono<Long> countByBookNameContaining(String bookName);
    
    // 获取所有记录的分页方法
    Flux<Book> findAllBy(Pageable pageable);
    
    // 统计所有记录数
    Mono<Long> count();
    
    // 根据websiteId查询
    Flux<Book> findByWebsiteId(Long websiteId);
    
    // 根据websiteId和书名模糊查询
    Flux<Book> findByWebsiteIdAndBookNameContaining(Long websiteId, String bookName, Pageable pageable);
    
    // 统计指定websiteId的记录数
    Mono<Long> countByWebsiteId(Long websiteId);
}