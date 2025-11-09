package com.book.reactive.repository;

import com.book.reactive.model.Chapter;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import org.springframework.data.r2dbc.repository.Query;


public interface ChapterRepository extends ReactiveCrudRepository<Chapter, Long> {
    // 根据书籍ID查询章节列表，按章节顺序排序
    Flux<Chapter> findByBookIdOrderByChapterOrderIdAsc(Long bookId);
    
    // 优化版本：只查询必要字段（id, chapterName, chapterOrderId）
    @Query("SELECT id, chapter_name, chapter_order_id, book_id FROM chapter WHERE book_id = :bookId ORDER BY chapter_order_id ASC")
    Flux<Chapter> findByBookIdOrderByChapterOrderIdAscWithProjection(Long bookId);
    
    // 根据书籍ID和章节URL精确查询
    Mono<Chapter> findByBookIdAndChapterUrl(Long bookId, String chapterUrl);
    
    // 根据书籍ID和章节ID查询章节
    Flux<Chapter> findByBookIdAndIdIn(Long bookId, java.util.Collection<Long> ids);
    
    // 根据章节名称模糊查询
    Flux<Chapter> findByChapterNameContaining(String keyword);
}