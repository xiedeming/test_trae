package com.book.reactive.repository;

import com.book.reactive.model.Chapter;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import org.springframework.data.r2dbc.repository.Query;


public interface ChapterRepository extends ReactiveCrudRepository<Chapter, Long> {
    // 根据书籍ID查询章节列表，按章节顺序排序
    @Query("SELECT id, chapter_name, chapter_order_id, book_id ,create_user_id ,create_user_name, create_time,modify_user_id,modify_user_name,modify_time, feature_code FROM chapter WHERE book_id = :bookId ORDER BY chapter_order_id ASC")
    Flux<Chapter> findByBookIdOrderByChapterOrderIdAsc(Long bookId);

    // 根据书籍ID查询章节列表，按章节顺序排序
    @Query("SELECT id, chapter_name, chapter_order_id, book_id ,create_user_id ,create_user_name, create_time,modify_user_id,modify_user_name,modify_time, feature_code,chapter_txt FROM chapter WHERE book_id = :bookId ")
    Flux<Chapter> findByBookIdOrder(Long bookId);

    // 优化版本：只查询必要字段（id, chapterName, chapterOrderId, bookId, featureCode）
    @Query("SELECT id, chapter_name, chapter_order_id, book_id, feature_code FROM chapter WHERE book_id = :bookId ORDER BY chapter_order_id ASC")
    Flux<Chapter> findByBookIdOrderByChapterOrderIdAscWithProjection(Long bookId);
    
    // 根据书籍ID和章节URL精确查询章节
    @Query("SELECT id, chapter_name, chapter_order_id, book_id ,create_user_id ,create_user_name, create_time,modify_user_id,modify_user_name,modify_time, feature_code FROM chapter WHERE book_id = :bookId AND chapter_url = :chapterUrl")
    Mono<Chapter> findByBookIdAndChapterUrl(Long bookId, String chapterUrl);
    
    // 根据书籍ID和章节ID查询章节
    @Query("SELECT id, chapter_name, chapter_order_id, book_id ,create_user_id ,create_user_name, create_time,modify_user_id,modify_user_name,modify_time, feature_code FROM chapter WHERE book_id = :bookId AND id IN :ids")
    Flux<Chapter> findByBookIdAndIdIn(Long bookId, java.util.Collection<Long> ids);
    
    // 根据章节名称模糊查询章节
    @Query("SELECT id, chapter_name, chapter_order_id, book_id ,create_user_id ,create_user_name, create_time,modify_user_id,modify_user_name,modify_time, feature_code FROM chapter WHERE chapter_name LIKE %:keyword%")
    Flux<Chapter> findByChapterNameContaining(String keyword);
    
    // 根据特征码查询章节
    @Query("SELECT id, chapter_name, chapter_order_id, book_id, feature_code FROM chapter WHERE feature_code = :featureCode")
    Mono<Chapter> findByFeatureCode(String featureCode);
 }