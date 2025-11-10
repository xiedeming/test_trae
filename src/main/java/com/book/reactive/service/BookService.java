package com.book.reactive.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;

import com.book.reactive.model.Book;
import com.book.reactive.repository.BookRepository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

@Service
public class BookService {
    @Autowired
    private BookRepository bookRepository;
    
    @Autowired
    private ReactiveRedisTemplate<String, Object> reactiveRedisTemplate;
    
    private static final String BOOK_CACHE_KEY_PREFIX = "book:";
    private static final String BOOK_NAME_KEY_PREFIX = "book:name:";

    // 查询所有书籍
    public Flux<Book> findAll() {
        return bookRepository.findAll();
    }
    
    // 根据网站ID和书籍名称精确查询
    public Mono<Book> findByWebsiteIdAndBookName(Long websiteId, String bookName) {
        return bookRepository.findByWebsiteIdAndBookName(websiteId, bookName);
    }

    // 根据ID查询书籍
    public Mono<Book> findById(Long id) {
        return bookRepository.findById(id);
    }

    // 根据websiteId查询书籍
    public Flux<Book> findByWebsiteId(Long websiteId) {
        return bookRepository.findByWebsiteId(websiteId);
    }

    // 保存书籍
    public Mono<Book> save(Book book) {
        LocalDateTime now = LocalDateTime.now();
        book.setCreateTime(now);
        book.setModifyTime(now);
        return bookRepository.save(book)
            .flatMap(savedBook -> {
                // 保存成功后更新Redis缓存
                String bookKey = BOOK_CACHE_KEY_PREFIX + savedBook.getId();
                String bookNameKey = BOOK_NAME_KEY_PREFIX + savedBook.getWebsiteId() + ":" + savedBook.getBookName();
                
                // 缓存书籍对象和书籍名称索引
                return reactiveRedisTemplate.opsForValue().set(bookKey, savedBook)
                    .then(reactiveRedisTemplate.opsForValue().set(bookNameKey, savedBook.getId()))
                    .then(Mono.just(savedBook));
            });
    }

    // 更新书籍
    public Mono<Book> update(Long id, Book book) {
        return bookRepository.findById(id)
                .flatMap(existingBook -> {
                    existingBook.setBookName(book.getBookName());
                    existingBook.setBookUrl(book.getBookUrl());
                    existingBook.setBookSummary(book.getBookSummary());
                    existingBook.setWebsiteId(book.getWebsiteId());
                    existingBook.setAuthor(book.getAuthor());
                    existingBook.setModifyUserId(book.getModifyUserId());
                    existingBook.setModifyUserName(book.getModifyUserName());
                    existingBook.setModifyTime(LocalDateTime.now());
                    return bookRepository.save(existingBook);
                });
    }

    // 删除书籍
    public Mono<Void> delete(Long id) {
        return bookRepository.deleteById(id);
    }
    
    // 批量删除书籍
    public Mono<Void> deleteBatch(Flux<Long> ids) {
        return ids.flatMap(bookRepository::deleteById).then();
    }
    
    // 分页查询（支持按书名搜索）
    public Mono<Map<String, Object>> findBooksWithPagination(int page, int size, String bookName) {
        Pageable pageable = PageRequest.of(page, size);
        Map<String, Object> result = new HashMap<>();
        
        if (bookName != null && !bookName.isEmpty()) {
            // 带书名条件的分页查询
            return bookRepository.findByBookNameContaining(bookName, pageable)
                    .collectList()
                    .flatMap(books -> {
                        result.put("books", books);
                        return bookRepository.countByBookNameContaining(bookName);
                    })
                    .map(count -> {
                        result.put("total", count);
                        result.put("page", page);
                        result.put("size", size);
                        result.put("pageSize", count/size + (count % size > 0 ? 1 : 0));
                        return result;
                    });
        } else {
            // 不带条件的分页查询
            return bookRepository.findAllBy(pageable)
                    .collectList()
                    .flatMap(books -> {
                        result.put("books", books);
                        return bookRepository.count();
                    })
                    .map(count -> {
                        result.put("total", count);
                        result.put("page", page);
                        result.put("size", size);
                        result.put("pageSize", count/size + (count % size > 0 ? 1 : 0));
                        return result;
                    });
        }
    }
    
    // 分页查询（按websiteId和书名搜索）
    public Mono<Map<String, Object>> findBooksByWebsiteWithPagination(int page, int size, Long websiteId, String bookName) {
        Pageable pageable = PageRequest.of(page, size);
        Map<String, Object> result = new HashMap<>();
        
        return bookRepository.findByWebsiteIdAndBookNameContaining(websiteId, bookName, pageable)
                .collectList()
                .flatMap(books -> {
                    result.put("books", books);
                    return bookRepository.countByWebsiteId(websiteId);
                })
                .map(count -> {
                    result.put("total", count);
                    result.put("page", page);
                    result.put("size", size);
                    result.put("websiteId", websiteId);
                    return result;
                });
    }
}