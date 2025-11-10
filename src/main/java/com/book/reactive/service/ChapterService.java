package com.book.reactive.service;

import com.book.reactive.model.Chapter;
import com.book.reactive.repository.ChapterRepository;
import com.book.reactive.util.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.time.LocalDateTime;

@Service
public class ChapterService {
    private final ChapterRepository chapterRepository;
    private final ReactiveRedisTemplate<String, Object> reactiveRedisTemplate;
    private final StringUtil stringUtil;
    
    private static final String CHAPTER_CACHE_KEY_PREFIX = "chapter:";
    private static final String CHAPTER_URL_KEY_PREFIX = "chapter:url:";

    @Autowired
    public ChapterService(ChapterRepository chapterRepository, ReactiveRedisTemplate<String, Object> reactiveRedisTemplate, StringUtil stringUtil) {
        this.chapterRepository = chapterRepository;
        this.reactiveRedisTemplate = reactiveRedisTemplate;
        this.stringUtil = stringUtil;
    }

    // 获取所有章节
    public Flux<Chapter> findAll() {
        return chapterRepository.findAll();
    }

    // 根据ID查询章节
    public Mono<Chapter> findById(Long id) {
        return chapterRepository.findById(id);
    }

    // 根据书籍ID查询章节列表（完整字段）
    public Flux<Chapter> findByBookId(Long bookId) {
        return chapterRepository.findByBookIdOrderByChapterOrderIdAsc(bookId);
    }

    public Flux<Chapter> findByBookIds(Long bookId) {
        return chapterRepository.findByBookIdOrder(bookId);
    }
    
    // 优化版本：只查询必要字段（id, chapterName, chapterOrderId）
    public Flux<Chapter> findByBookIdWithProjection(Long bookId) {
        return chapterRepository.findByBookIdOrderByChapterOrderIdAscWithProjection(bookId);
    }
    
    // 根据书籍ID和章节URL精确查询
    public Mono<Chapter> findByBookIdAndChapterUrl(Long bookId, String chapterUrl) {
        return chapterRepository.findByBookIdAndChapterUrl(bookId, chapterUrl);
    }
    
    // 根据特征码查询章节（用于内容重复检查）
    public Mono<Chapter> findByFeatureCode(String featureCode) {
        return chapterRepository.findByFeatureCode(featureCode);
    }

    // 保存章节
    public Mono<Chapter> save(Chapter chapter) {
        LocalDateTime now = LocalDateTime.now();
        chapter.setCreateTime(now);
        chapter.setModifyTime(now);
        
        // 生成并设置章节特征码
        if (chapter.getChapterTxt() != null && chapter.getFeatureCode() == null) {
            String featureCode = stringUtil.extractFeatureCode(chapter.getChapterTxt());
            chapter.setFeatureCode(featureCode);
        }
        
        return chapterRepository.save(chapter)
            .flatMap(savedChapter -> {
                // 保存成功后更新Redis缓存
                String chapterKey = CHAPTER_CACHE_KEY_PREFIX + savedChapter.getId();
                String featureCode = stringUtil.extractFeatureCode(savedChapter.getChapterTxt());
                String chapterFeatureCodeKey = "chapter:feature:" + featureCode;  
                
                // 缓存章节对象
                return reactiveRedisTemplate.opsForValue().set(chapterKey, savedChapter)
                    // 缓存章节URL索引（使用特征码作为主要缓存键）
                    .then(reactiveRedisTemplate.opsForValue().set(chapterFeatureCodeKey, savedChapter.getId()))
                    // 返回保存的章节对象
                    .then(Mono.just(savedChapter));
            });
    }

    // 更新章节
    public Mono<Chapter> update(Long id, Chapter chapter) {
        return chapterRepository.findById(id)
                .flatMap(existingChapter -> {
                    // 当章节内容更新时，重新生成特征码
                    if (chapter.getChapterTxt() != null && !chapter.getChapterTxt().equals(existingChapter.getChapterTxt())) {
                        String featureCode = stringUtil.extractFeatureCode(chapter.getChapterTxt());
                        existingChapter.setFeatureCode(featureCode);
                    }
                    
                    existingChapter.setChapterUrl(chapter.getChapterUrl());
                    existingChapter.setChapterName(chapter.getChapterName());
                    existingChapter.setChapterOrderId(chapter.getChapterOrderId());
                    existingChapter.setChapterTxt(chapter.getChapterTxt());
                    existingChapter.setModifyUserId(chapter.getModifyUserId());
                    existingChapter.setModifyUserName(chapter.getModifyUserName());
                    existingChapter.setModifyTime(LocalDateTime.now());
                    
                    return chapterRepository.save(existingChapter)
                            .doOnSuccess(updatedChapter -> {
                                // 更新Redis缓存
                                String cacheKey = CHAPTER_CACHE_KEY_PREFIX + updatedChapter.getId();
                                reactiveRedisTemplate.opsForValue().set(cacheKey, updatedChapter);
                                String chapterFeatureCodeKey = "chapter:feature:" + updatedChapter.getFeatureCode();
                                reactiveRedisTemplate.opsForValue().set(chapterFeatureCodeKey, updatedChapter.getId());
                            });
                });
    }

    // 删除章节
    public Mono<Void> deleteById(Long id) {
        return chapterRepository.deleteById(id);
    }

    // 批量删除章节
    public Mono<Void> deleteByIdIn(Iterable<Long> ids) {
        return chapterRepository.deleteAllById(ids);
    }

    // 根据章节名称搜索
    public Flux<Chapter> searchByChapterName(String keyword) {
        return chapterRepository.findByChapterNameContaining(keyword);
    }
}