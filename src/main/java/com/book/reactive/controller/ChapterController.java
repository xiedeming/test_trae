package com.book.reactive.controller;

import com.book.reactive.model.Chapter;
import com.book.reactive.service.ChapterService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/chapters")
public class ChapterController {
    @Autowired
    private ChapterService chapterService;

    // 获取所有章节
    @GetMapping
    public Flux<Chapter> getAllChapters() {
        return chapterService.findAll();
    }

    // 根据ID查询章节
    @GetMapping("/{id}")
    public Mono<ResponseEntity<Chapter>> getChapterById(@PathVariable Long id) {
        return chapterService.findById(id)
                .map(chapter -> ResponseEntity.ok(chapter))
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    // 根据书籍ID查询章节列表（优化版本：只查询必要字段）
    @GetMapping("/book/{bookId}")
    public Flux<Chapter> getChaptersByBookId(@PathVariable Long bookId) {
        return chapterService.findByBookIdWithProjection(bookId);
    }

    // 创建章节
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<Chapter> createChapter(@RequestBody Chapter chapter) {
        return chapterService.save(chapter);
    }

    // 更新章节
    @PutMapping("/{id}")
    public Mono<ResponseEntity<Chapter>> updateChapter(@PathVariable Long id, @RequestBody Chapter chapter) {
        return chapterService.update(id, chapter)
                .map(updatedChapter -> ResponseEntity.ok(updatedChapter))
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    // 删除章节
    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Mono<Void> deleteChapter(@PathVariable Long id) {
        return chapterService.deleteById(id);
    }

    // 批量删除章节
    @DeleteMapping("/batch")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Mono<Void> deleteChapters(@RequestBody Iterable<Long> ids) {
        return chapterService.deleteByIdIn(ids);
    }

    // 搜索章节
    @GetMapping("/search")
    public Flux<Chapter> searchChapters(@RequestParam String keyword) {
        return chapterService.searchByChapterName(keyword);
    }
}